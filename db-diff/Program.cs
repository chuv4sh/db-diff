using System.ComponentModel.DataAnnotations;
using Microsoft.Data.SqlClient;
using Npgsql;
using CommandLine;
using database_diff;

class Program
{
    private class Options
    {
        [Option('m', "mssql-connection", Required = true, HelpText = "Connection to MSSQL")]
        public string MssqlConnectionString { get; set; }

        [Option('q', "mssql-query", Required = true, HelpText = "MSSQL query")]
        public string MssqlQuery { get; set; }

        [Option("mssql-parameters", Separator = ';', Required = false, HelpText = "MSSQL parameters")]
        public IEnumerable<string>? MssqlParameters { get; set; }

        [Option("mssql-keycolumns", Separator = ',', Required = false, HelpText = "MSSQL key columns")]
        public IEnumerable<string>? MssqlKeyColumns { get; set; }

        [Option('p', "pgsql-connection", Required = true, HelpText = "MSSQL key columns")]
        public string PgsqlConnectionString { get; set; }

        [Option("pgsql-query", HelpText = "PostgreSQL query")]
        public string? PgsqlQuery { get; set; }

        [Option("pgsql-parameters", Separator = ';', Required = false, HelpText = "MSSQL parameters")]
        public IEnumerable<string>? PgsqlParameters { get; set; }

        [Option("pgsql-keycolumns", Separator = ',', Required = false, HelpText = "PostgreSQL key columns")]
        public IEnumerable<string>? PgsqlKeyColumns { get; set; }
    }


    static void Main(string[] args)
    {
        Parser.Default.ParseArguments<Options>(args).WithParsed(RunComparison);
    }

    static void RunComparison(Options options)
    {
        var mssqlParametersDictionary = new Dictionary<string, object>();
        if (options.MssqlParameters != null)
        {
            foreach (var parameter in options.MssqlParameters)
            {
                var parts = parameter.Split('=');
                if (parts.Length == 2)
                    mssqlParametersDictionary.Add(parts[0], parts[1]);
            }
        }

        var pgsqlParametersDictionary = new Dictionary<string, object>();
        if (options.PgsqlParameters != null)
        {
            foreach (var parameter in options.PgsqlParameters)
            {
                var parts = parameter.Split('=');
                if (parts.Length == 2)
                    pgsqlParametersDictionary.Add(parts[0], parts[1]);
            }
        }

        var dbc = new DatabaseComparer(options.MssqlConnectionString, options.MssqlQuery, options.PgsqlConnectionString,
            options.PgsqlQuery, options.MssqlKeyColumns.ToArray(), options.PgsqlKeyColumns.ToArray(),
            mssqlParametersDictionary, pgsqlParametersDictionary);
        dbc.CompareQueryResults();
    }
}