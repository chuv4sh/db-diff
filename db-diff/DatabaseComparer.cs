using System.Collections.Immutable;
using System.Data;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.IdentityModel.Tokens;
using Npgsql;

namespace database_diff;

public class DatabaseComparer
{
    private readonly string _pgsqlConnectionString;
    private readonly string _pgsqlQuery;
    private readonly Dictionary<string, object> _pgsqlParameters;

    private readonly string _mssqlConnectionString;
    private readonly string _mssqlQuery;
    private readonly Dictionary<string, object> _mssqlParameters;

    private readonly string[] _mssqlKeyColumns;
    private readonly string[]? _pgsqlKeyColumns;

    private bool _areEqual = true;

    public DatabaseComparer(string mssqlConnectionString,
        string mssqlQuery, string pgsqlConnectionString, string? pgsqlQuery,
        string[]? mssqlKeyColumns, string[]? pgsqlKeyColumns,
        Dictionary<string, object>? mssqlParameters, Dictionary<string, object>? pgsqlParameters)
    {
        this._mssqlParameters = mssqlParameters;
        this._mssqlConnectionString = mssqlConnectionString;
        this._mssqlQuery = mssqlQuery;
        this._pgsqlConnectionString = pgsqlConnectionString;
        this._pgsqlQuery = pgsqlQuery ?? mssqlQuery;
        this._mssqlKeyColumns = mssqlKeyColumns;
        this._pgsqlParameters = pgsqlParameters ?? mssqlParameters;
        this._pgsqlKeyColumns = pgsqlKeyColumns;
    }

    public void CompareQueryResults()
    {
        try
        {
            var mssqlDataTable = ExecuteMssqlQuery();
            var pgsqlDataTable = ExecutePgsqlQuery();

            if (mssqlDataTable.Rows.Count != pgsqlDataTable.Rows.Count)
            {
                _areEqual = false;
                Console.WriteLine($"Number of mssql rows: {mssqlDataTable.Rows.Count}");
                Console.WriteLine($"Number of pgsql rows: {pgsqlDataTable.Rows.Count}");
            }

            // without key columns
            if (_mssqlKeyColumns.IsNullOrEmpty() || _pgsqlKeyColumns.IsNullOrEmpty())
            {
                var mssqlDictionary = GetDictionary(mssqlDataTable);
                var pgsqlDiffRows = CompareTablesWithoutKeyColumns(mssqlDictionary, pgsqlDataTable);

                if (_areEqual)
                {
                    Console.WriteLine("Tables are equal");
                }
                else
                {
                    Console.WriteLine("Tables are not equal");
                    var mssqlDiffRows = new Dictionary<int, DataRow>();
                    foreach (var item in mssqlDictionary)
                    {
                        mssqlDiffRows.Add(item.Value.Item1, item.Value.Item2);
                    }

                    PrintMismatchedRows(mssqlDiffRows, pgsqlDiffRows);
                    File.WriteAllText("output.html", MismatchedRowsToHtml(mssqlDiffRows, pgsqlDiffRows));
                }
            }
            // with key columns
            else
            {
                var mssqlMissingRows = new Dictionary<int, DataRow>();
                var pgsqlMissingRows = new Dictionary<int, DataRow>();
                var mssqlMismatchedRows = new Dictionary<int, DataRow>();
                var pgsqlMismatchedRows = new Dictionary<int, DataRow>();
                // to mark mismatched columns
                var mssqlMismatchedColumns = new Dictionary<int, HashSet<int>>();
                var pgsqlMismatchedColumns = new Dictionary<int, HashSet<int>>();

                CompareTablesWithKeyColumns(mssqlDataTable, pgsqlDataTable, mssqlMissingRows, pgsqlMissingRows,
                    mssqlMismatchedColumns, pgsqlMismatchedColumns,
                    mssqlMismatchedRows, pgsqlMismatchedRows);
                if (_areEqual)
                {
                    Console.WriteLine("Tables are equal");
                }
                else
                {
                    Console.WriteLine("Tables are not equal");
                    PrintMismatchedRows(mssqlMissingRows, pgsqlMissingRows, mssqlMismatchedRows, pgsqlMismatchedRows,
                        mssqlMismatchedColumns, pgsqlMismatchedColumns);
                    File.WriteAllText("output.html", MismatchedRowsToHtml(mssqlMissingRows, pgsqlMissingRows,
                        mssqlMismatchedRows, pgsqlMismatchedRows,
                        mssqlMismatchedColumns, pgsqlMismatchedColumns));
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine("Error occurred: " + e.Message);
        }
    }

    private void CompareTablesWithKeyColumns(DataTable mssqlDataTable, DataTable pgsqlDataTable,
        Dictionary<int, DataRow> mssqlMissingRows, Dictionary<int, DataRow> pgsqlMissingRows,
        Dictionary<int, HashSet<int>> mssqlMismatchedColumns, Dictionary<int, HashSet<int>> pgsqlMismatchedColumns,
        Dictionary<int, DataRow> mssqlMismatchedRows, Dictionary<int, DataRow> pgsqlMismatchedRows)
    {
        for (int i = 0; i < mssqlDataTable.Rows.Count; i++)
        {
            var keyValueArray = new object[_mssqlKeyColumns.Length];
            for (int j = 0; j < _mssqlKeyColumns.Length; j++)
            {
                keyValueArray[j] = mssqlDataTable.Rows[i][_mssqlKeyColumns[j]];
            }

            if (pgsqlDataTable.Rows.Contains(keyValueArray))
            {
                var pgsqlFoundDataRow = pgsqlDataTable.Rows.Find(keyValueArray);
                var pgsqlFoundDataRowIndex = pgsqlDataTable.Rows.IndexOf(pgsqlFoundDataRow);
                if (!AreDataRowsEqual(mssqlDataTable.Rows[i], pgsqlFoundDataRow))
                {
                    DiffDataRows(mssqlDataTable.Rows[i], pgsqlFoundDataRow, mssqlMismatchedColumns,
                        pgsqlMismatchedColumns, i + 1,
                        pgsqlFoundDataRowIndex + 1);
                    mssqlMismatchedRows.Add(i + 1, mssqlDataTable.Rows[i]);
                    pgsqlMismatchedRows.Add(pgsqlFoundDataRowIndex + 1, pgsqlFoundDataRow);
                }

                pgsqlFoundDataRow.SetModified();
            }
            else
            {
                mssqlMissingRows.Add(i + 1, mssqlDataTable.Rows[i]);
            }
        }

        if (!pgsqlMissingRows.IsNullOrEmpty() || !mssqlMissingRows.IsNullOrEmpty())
            _areEqual = false;

        foreach (DataRow row in pgsqlDataTable.Rows)
        {
            if (row.RowState != DataRowState.Modified)
                pgsqlMissingRows.Add(pgsqlDataTable.Rows.IndexOf(row) + 1, row);
        }
    }

    private void DiffDataRows(DataRow mssqlDataRow, DataRow pgsqlFoundDataRow,
        Dictionary<int, HashSet<int>> mssqlMismatchedColumns, Dictionary<int, HashSet<int>> pgsqlMismatchedColumns,
        int mssqlRowNumber, int pgsqlRowNumber)
    {
        var mssqlDiffColumnsSet = new HashSet<int>();
        var pgsqlDiffColumnsSet = new HashSet<int>();
        for (int i = 0; i < mssqlDataRow.ItemArray.Length; i++)
        {
            if (!mssqlDataRow.ItemArray[i].Equals(pgsqlFoundDataRow.ItemArray[i]))
            {
                mssqlDiffColumnsSet.Add(i);
                pgsqlDiffColumnsSet.Add(i);
            }
        }

        mssqlMismatchedColumns.Add(mssqlRowNumber, mssqlDiffColumnsSet);
        pgsqlMismatchedColumns.Add(pgsqlRowNumber, pgsqlDiffColumnsSet);
    }
    
    private bool AreDataRowsEqual(DataRow mssqlDataRow, DataRow pgsqlDataRow)
    {
        for (int i = 0; i < mssqlDataRow.ItemArray.Length; i++)
        {
            if (!mssqlDataRow.ItemArray[i].Equals(pgsqlDataRow.ItemArray[i]))
                return false;
        }

        return true;
    }
    

    private void PrintMismatchedRows(Dictionary<int, DataRow> mssqlMissingRows,
        Dictionary<int, DataRow> pgsqlMissingRows, Dictionary<int, DataRow> mssqlMismatchedRows = null,
        Dictionary<int, DataRow> pgsqlMismatchedRows = null,
        Dictionary<int, HashSet<int>> mssqlMismatchedColumns = null, Dictionary<int, HashSet<int>> pgsqlMismatchedColumns = null)
    {
        var keys = new List<int>(mssqlMissingRows.Keys);
        var printedKeys = new HashSet<int>();
        keys.AddRange(pgsqlMissingRows.Keys);
        if (!mssqlMismatchedRows.IsNullOrEmpty()) keys.AddRange(mssqlMismatchedRows.Keys);
        if (!pgsqlMismatchedRows.IsNullOrEmpty()) keys.AddRange(pgsqlMismatchedRows.Keys);
        keys.Sort();
        foreach (var key in keys)
        {
            if (!printedKeys.Contains(key))
            {
                if (!mssqlMismatchedRows.IsNullOrEmpty() && mssqlMismatchedRows.ContainsKey(key))
                {
                    Console.Write($"  row {key}: ");
                    PrintDataRow(mssqlMismatchedRows[key], mssqlMismatchedColumns[key]);
                }

                if (!pgsqlMismatchedRows.IsNullOrEmpty() && pgsqlMismatchedRows.ContainsKey(key))
                {
                    Console.Write($"  row {key}: ");
                    PrintDataRow(pgsqlMismatchedRows[key], pgsqlMismatchedColumns[key]);
                }

                if (mssqlMissingRows.ContainsKey(key))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.Write($"- row {key}: ");
                    PrintDataRow(mssqlMissingRows[key]);
                    Console.ResetColor();
                }

                if (pgsqlMissingRows.ContainsKey(key))
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.Write($"+ row {key}: ");
                    PrintDataRow(pgsqlMissingRows[key]);
                    Console.ResetColor();
                }

                printedKeys.Add(key);
            }
        }
    }

    private void PrintDataRow(DataRow dataRow)
    {
        var cells = dataRow.ItemArray;
        foreach (var cell in cells)
        {
            Console.Write(cell.Equals(DBNull.Value) ? "null\t" : $"{cell}\t");
        }

        Console.WriteLine();
    }

    private void PrintDataRow(DataRow dataRow, HashSet<int> markedColumns)
    {
        var cells = dataRow.ItemArray;

        for (int i = 0; i < cells.Length; i++)
        {
            if (markedColumns.Contains(i))
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.Write(cells[i].Equals(DBNull.Value) ? "null\t" : $"{cells[i]}\t");
                Console.ResetColor();
            }
            else
            {
                Console.Write(cells[i].Equals(DBNull.Value) ? "null\t" : $"{cells[i]}\t");
            }
        }

        Console.WriteLine();
    }

    private string MismatchedRowsToHtml(Dictionary<int, DataRow> mssqlMissingRows,
        Dictionary<int, DataRow> pgsqlMissingRows, Dictionary<int, DataRow> mssqlMismatchedRows = null,
        Dictionary<int, DataRow> pgsqlMismatchedRows = null,
        Dictionary<int, HashSet<int>> mssqlMismatchedColumns = null,
        Dictionary<int, HashSet<int>> pgsqlMismatchedColumns = null)
    {
        var writer = new StringWriter();
        writer.WriteLine("<!DOCTYPE html>");
        writer.WriteLine("<html>");
        writer.WriteLine("<head>");
        writer.WriteLine("<style>");
        writer.WriteLine("table { width: 100%; border-collapse: collapse; }");
        writer.WriteLine(
            "th, td { padding: 3px; text-align: left; border-bottom: 1px solid #ddd; border-right: 1px solid #ddd; }");
        writer.WriteLine("tr:hover { background-color: #f5f5f5; }");
        writer.WriteLine(".red-row { background-color: #fa8072;}");
        writer.WriteLine(".green-row { background-color: lawngreen;}");
        writer.WriteLine(".yellow-cell { background-color: yellow;}");
        writer.WriteLine("</style>");
        writer.WriteLine("</head>");
        writer.WriteLine("<body>");
        writer.WriteLine("<table>");

        writer.WriteLine("<tr>");
        writer.WriteLine("<th>DBMS</th>");
        writer.WriteLine("<th>Row number</th>");
        foreach (DataColumn column in mssqlMissingRows.Values.First().Table.Columns)
        {
            writer.WriteLine("<th>{0}</th>", column.ColumnName);
        }

        writer.WriteLine("</tr>");

        var keys = new List<int>(mssqlMissingRows.Keys);
        var printedKeys = new HashSet<int>();
        keys.AddRange(pgsqlMissingRows.Keys);
        if (!mssqlMismatchedRows.IsNullOrEmpty()) keys.AddRange(mssqlMismatchedRows.Keys);
        if (!pgsqlMismatchedRows.IsNullOrEmpty()) keys.AddRange(pgsqlMismatchedRows.Keys);
        keys.Sort();
        foreach (var key in keys)
        {
            if (!printedKeys.Contains(key))
            {
                if (!mssqlMismatchedRows.IsNullOrEmpty() && mssqlMismatchedRows.ContainsKey(key))
                {
                    writer.WriteLine("<tr>");
                    writer.WriteLine("<td>MSSQL</td>");
                    writer.WriteLine("<td>{0}</td>", key);
                    writer.WriteLine(DataRowToHtml(mssqlMismatchedRows[key], mssqlMismatchedColumns[key]));
                }

                if (!pgsqlMismatchedRows.IsNullOrEmpty() && pgsqlMismatchedRows.ContainsKey(key))
                {
                    writer.WriteLine("<tr>");
                    writer.WriteLine("<td>PgSQL</td>");
                    writer.WriteLine("<td>{0}</td>", key);
                    writer.WriteLine(DataRowToHtml(pgsqlMismatchedRows[key], pgsqlMismatchedColumns[key]));
                }

                if (mssqlMissingRows.ContainsKey(key))
                {
                    writer.WriteLine("<tr class=\"red-row\">");
                    writer.WriteLine("<td>MSSQL</td>");
                    writer.WriteLine("<td>{0}</td>", key);
                    writer.WriteLine(DataRowToHtml(mssqlMissingRows[key]));
                }

                if (pgsqlMissingRows.ContainsKey(key))
                {
                    writer.WriteLine("<tr class=\"green-row\">");
                    writer.WriteLine("<td>PgSQL</td>");
                    writer.WriteLine("<td>{0}</td>", key);
                    writer.WriteLine(DataRowToHtml(pgsqlMissingRows[key]));
                }

                printedKeys.Add(key);
            }
        }

        writer.WriteLine("</table>");
        writer.WriteLine("</body>");
        writer.WriteLine("</html>");

        return writer.ToString();
    }

    private string DataRowToHtml(DataRow dataRow)
    {
        var writer = new StringWriter();
        var cells = dataRow.ItemArray;
        foreach (var cell in cells)
        {
            writer.WriteLine("<td>{0}</td>", cell.Equals(DBNull.Value) ? "null" : $"{cell}");
        }

        writer.Write("</tr>");

        return writer.ToString();
    }

    private string DataRowToHtml(DataRow dataRow, HashSet<int> markedColumns)
    {
        var writer = new StringWriter();
        var cells = dataRow.ItemArray;

        for (int i = 0; i < cells.Length; i++)
        {
            if (markedColumns.Contains(i))
            {
                writer.WriteLine("<td class=\"yellow-cell\">{0}</td>",
                    cells[i].Equals(DBNull.Value) ? "null" : $"{cells[i]}");
            }
            else
            {
                writer.WriteLine("<td>{0}</td>", cells[i].Equals(DBNull.Value) ? "null" : $"{cells[i]}");
            }
        }

        writer.Write("</tr>");

        return writer.ToString();
    }

    private Dictionary<string, (int, DataRow)> GetDictionary(DataTable dataTable)
    {
        var dictionary = new Dictionary<string, (int, DataRow)>();
        for (int i = 0; i < dataTable.Rows.Count; i++)
        {
            dictionary.Add(GetDataRowHashcode(dataTable.Rows[i]), (i + 1, dataTable.Rows[i]));
        }

        return dictionary;
    }

    private Dictionary<int, DataRow> CompareTablesWithoutKeyColumns(Dictionary<string, (int, DataRow)> mssqlDictionary,
        DataTable pgsqlDataTable)
    {
        var pgsqlDiffDictionary = new Dictionary<int, DataRow>();

        for (int i = 0; i < pgsqlDataTable.Rows.Count; i++)
        {
            var dataRowHashCode = GetDataRowHashcode(pgsqlDataTable.Rows[i]);
            if (mssqlDictionary.ContainsKey(dataRowHashCode))
            {
                mssqlDictionary.Remove(dataRowHashCode);
            }
            else
            {
                pgsqlDiffDictionary.Add(i + 1, pgsqlDataTable.Rows[i]);
            }
        }

        if (!pgsqlDiffDictionary.IsNullOrEmpty())
            _areEqual = false;
        return pgsqlDiffDictionary;
    }

    private string GetDataRowHashcode(DataRow dataRow)
    {
        var stringBuilder = new StringBuilder();

        foreach (object value in dataRow.ItemArray)
        {
            stringBuilder.Append(value.ToString());
        }

        var input = stringBuilder.ToString();

        using (MD5 md5 = MD5.Create())
        {
            byte[] hashBytes = md5.ComputeHash(Encoding.UTF8.GetBytes(input));
            StringBuilder hashString = new StringBuilder();
            foreach (byte b in hashBytes)
            {
                hashString.Append(b.ToString("x2"));
            }

            return hashString.ToString();
        }
    }

    private DataTable ExecuteMssqlQuery()
    {
        using var connection = new SqlConnection(_mssqlConnectionString);
        using var command = new SqlCommand(_mssqlQuery, connection);
        using var adapter = new SqlDataAdapter(command);
        var dataTable = new DataTable();
        connection.Open();
        if (!_mssqlParameters.IsNullOrEmpty())
            foreach (var parameter in _mssqlParameters)
            {
                command.Parameters.AddWithValue(parameter.Key, parameter.Value);
            }

        adapter.Fill(dataTable);
        if (!_mssqlKeyColumns.IsNullOrEmpty())
        {
            var primaryKeysColumns = new List<DataColumn>();
            foreach (string keyColumn in _mssqlKeyColumns)
            {
                primaryKeysColumns.Add(dataTable.Columns[keyColumn]);
            }

            dataTable.PrimaryKey = primaryKeysColumns.ToArray();
        }

        connection.Close();
        return dataTable;
    }

    private DataTable ExecutePgsqlQuery()
    {
        using var connection = new NpgsqlConnection(_pgsqlConnectionString);
        using var command = new NpgsqlCommand(_pgsqlQuery, connection);
        using var adapter = new NpgsqlDataAdapter(command);
        var dataTable = new DataTable();
        connection.Open();
        if (!_pgsqlParameters.IsNullOrEmpty())
            foreach (var parameter in _pgsqlParameters)
            {
                command.Parameters.AddWithValue(parameter.Key, parameter.Value);
            }

        adapter.Fill(dataTable);
        if (!_pgsqlKeyColumns.IsNullOrEmpty())
        {
            var primaryKeysColumns = new List<DataColumn>();
            foreach (string keyColumn in _pgsqlKeyColumns)
            {
                primaryKeysColumns.Add(dataTable.Columns[keyColumn]);
            }

            dataTable.PrimaryKey = primaryKeysColumns.ToArray();
        }

        connection.Close();
        return dataTable;
    }
}