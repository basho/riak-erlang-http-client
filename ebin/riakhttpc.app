{application, riakhttpc,
 [
  {description, "Riak HTTP Client"},
  {vsn, "1.4"},
  {modules, [
             rhc,
             rhc_ts,
             rhc_bucket,
             rhc_dt,
             rhc_listkeys,
             rhc_index,
             rhc_mapred,
             rhc_obj
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  crypto,
                  ibrowse
                 ]},
  {env, []}
 ]}.
