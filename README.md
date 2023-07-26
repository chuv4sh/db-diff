# db-diff
Утилита предназначена для сравнения данных между двумя базами данных: Microsoft SQL Server (MSSQL) и PostgreSQL (PgSQL). Инструмент позволяет указать строки подключения, запросы, ключевые столбцы и параметры для каждой базы данных, которые будут сравниваться. Результаты сравнения отображаются в консоли и могут быть сохранены в HTML-файл.
## Как использовать
Утилита представляет собой консольное приложение и может быть запущена из командной строки с соответствующими параметрами. Ниже приведено руководство по использованию приложения.
### Параметры командной строки
Приложение принимает следующие параметры командной строки:

-m, --mssql-connection: Обязательный. Строка подключения к базе данных MSSQL.

-p, --pgsql-connection: Обязательный. Строка подключения к базе данных PgSQL.

-q, --mssql-query: Обязательный. SQL-запрос для выполнения на базе данных MSSQL для сравнения.

--pgsql-query: Необязательный. SQL-запрос для выполнения на базе данных PgSQL для сравнения. Если не указан, будет использован тот же запрос, что и для MSSQL.

--mssql-parameters: Необязательный. Список параметров через точку с запятой в формате "имя_параметра1=значение_параметра1;имя_параметра2=значение_параметра2", которые будут использоваться в SQL-запросе MSSQL.

--pgsql-parameters: Необязательный. Список параметров через точку с запятой в формате "имя_параметра1=значение_параметра1;имя_параметра2=значение_параметра2", которые будут использоваться в SQL-запросе PgSQL.

--mssql-keycolumns: Необязательный. Список ключевых столбцов, используемых для однозначной идентификации строк в таблице MSSQL.

--pgsql-keycolumns: Необязательный. Список ключевых столбцов, используемых для однозначной идентификации строк в таблице PgSQL.

-o, --output-filename: Необязательный. Название HTML-файла, в который будут сохранены результаты сравнения. Если не указан, будет использовано имя "output.html" по умолчанию.

### Пример использования

db-diff --mssql-connection "mssql connection string" --pgsql-connection "pgsql connection string" --mssql-query "SELECT * FROM test WHERE b = @value;" --pgsql-query "SELECT * FROM test WHERE b = :value;" --mssql-parameters @value=foo --pgsql-parameters value=foo --mssql-keycolumns id --pgsql-keycolumns id

