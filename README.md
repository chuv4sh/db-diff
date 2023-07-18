# db-diff
## Usage

db-diff --mssql-connection "mssql connection string" --pgsql-connection "pgsql connection string"
--mssql-query "SELECT * FROM test WHERE b = @value;" --pgsql-query "SELECT * FROM test WHERE b = :value;"
--mssql-parameters @value=foo --pgsql-parameters value=foo
--mssql-keycolumns id --pgsql-keycolumns id
