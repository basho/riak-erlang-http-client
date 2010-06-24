{application, riakhttpc,
 [
  {description, "Riak HTTP Client"},
  {vsn, "1"},
  {modules, [
             rhc
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  ibrowse
                 ]},
  {env, []}
 ]}.
