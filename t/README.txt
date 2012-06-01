To automate testing:
  Set CRO_DB_MODE=s to skip all DB testing
   -OR-

  Set CRO_DB_MODE=d to use a custom DBI DSN
    -THEN-
      Set CRO_DB_DSN='string'
      Set CRO_DB_USERNAME=user
      Set CRO_DB_PASSWORD=pass
   -OR-

  Set CRO_DB_MODE=g to generate a testing database
    -THEN-
      Set CRO_DB_RDBMS={p,s}

-THEN-
  If you want to keep the database running after testing,
      Set CRO_DB_KEEP_RUNNING=1
  otherwise t/Z99-shutdown.t will shutdown the test database cluster.

These variables are used in the t/tlib/CrormTest/DB.pm handle, and the t/Z99-shutdown.t test script.
