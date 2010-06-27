-define(DEFAULT_TIMEOUT, 60000).

-record(rhc, {ip,
              port,
              prefix,
              options}).
