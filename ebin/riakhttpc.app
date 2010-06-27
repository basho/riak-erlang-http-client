{application, riakhttpc,
 [
  {description, "Riak HTTP Client"},
  {vsn, "1"},
  {modules, [
             rhc,
             rhc_bucket,
             rhc_listkeys,
             rhc_mapred,
             rhc_obj
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  ibrowse
                 ]},
  {env, []}
 ]}.
