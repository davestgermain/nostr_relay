import logging
import collections
from time import perf_counter


class BaseRateLimiter:
    def __init__(self, options=None):
        self.log = logging.getLogger('nostr_relay.limiter')

    def cleanup(self):
        pass


class RateLimiter(BaseRateLimiter):
    """
    A configurable rate limiter
    
    The options dict looks like:
    {
        "global": {
            "EVENT": "10/s,100/hour,40/min"
        },
        "ip": {
            "REQ": "10/minute"
        }
    }
    """
    def __init__(self, options=None):
        super().__init__(options)
        self.log = logging.getLogger('nostr_relay.limiter')
        self.rules = self.parse_options(options or {})
        self.recent_commands = collections.defaultdict(lambda: collections.defaultdict(collections.deque))

    def parse_options(self, options):
        rules = {}
        for category, value in options.items():
            category_rules = {}
            for event, rule in value.items():
                category_rules[event] = self.parse_option(rule)
            rules[category] = category_rules
        self.log.debug("Parsed rate limits: rules:%s", rules)
        self.log.info("Rate limiter enabled")
        return rules

    def parse_option(self, option):
        rules = []
        for rule in option.split(','):
            if not rule:
                continue
            try:
                freq, interval = rule.split('/')
            except ValueError:
                continue
            interval = interval.lower()
            if interval in ('s', 'second', 'sec'):
                interval = 1
            elif interval in ('m', 'minute', 'min'):
                interval = 60
            elif interval in ('h', 'hour', 'hr'):
                interval = 3600
            else:
                raise ValueError(interval)
            rules.append((interval, int(freq)))
        rules.sort(reverse=True)
        return rules

    def evaluate_rules(self, rules, timestamps):
        now = perf_counter()
        if timestamps:
            if (now - timestamps[0]) > max(rules)[0]:
                timestamps.clear()
            else:
                for interval, freq in rules:
                    count = 0
                    for ts in timestamps:
                        if (now - ts) < interval:
                            count += 1
                        if count == freq:
                            self.log.debug("%d/%d", freq, interval)
                            return True
        return False

    def is_limited(self, ip_address, message):
        command = message[0]
        self.log.debug("Checking limits for %s %s", command, ip_address)
        if not self.rules:
            return False
        matches = []
        for key in (ip_address, 'global', 'ip'):
            rules = self.rules.get(key, {})
            if rules:
                if command in rules:
                    recent_timestamps = self.recent_commands[key if key == 'global' else ip_address][command]
                    if self.evaluate_rules(rules[command], recent_timestamps):
                        self.log.warning("Rate limiting for %s %s", command, rules[command])
                        return True
                    recent_timestamps.insert(0, perf_counter())
                    if '.' in key:
                        # specific ip address rules take precedence
                        # stop evaluating global and ip rules
                        return False
        return False

    def cleanup(self):
        max_interval = 0
        if not self.rules.get('ip'):
            return
        for rules in self.rules['ip'].values():
            rule_res = max(rules)[0]
            max_interval = max(rule_res, max_interval)

        now = perf_counter()
        to_del = []
        for ip, commands in self.recent_commands.items():
            if ip == 'global':
                continue

            cleared = []
            for cmd, ts in commands.items():
                if (not ts) or (now - ts[0]) > max_interval:
                    ts.clear()
                    cleared.append(cmd)
            if len(cleared) == len(commands):
                to_del.append(ip)
        for k in to_del:
            try:
                del self.recent_commands[k]
            except KeyError:
                pass


class NullRateLimiter(BaseRateLimiter):
    """
    A rate limiter that does nothing
    """
    def is_limited(self, ip_address, message):
        return False


def get_rate_limiter(options):
    """
    Return a rate limiter instance.
    If options["rate_limiter_class"] is set, will import and use that implementation.
    Otherwise, we'll use RateLimiter

    If options["rate_limits"] is not set, return NullRateLimiter
    """

    if 'rate_limits' in options:
        classpath = options.get('rate_limiter_class', 'nostr_relay.rate_limiter:RateLimiter')
        modulename, classname = classpath.split(':', 1)
        if modulename == 'nostr_relay.rate_limiter':
            classobj = globals()[classname]
        else:
            import importlib
            module = importlib.import_module(modulename)
            classobj = getattr(module, classname)
    else:
        classobj = NullRateLimiter
    return classobj(options.get('rate_limits', {}))
