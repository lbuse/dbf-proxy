using System;
using System.Text;
using System.IO;

using System.Data;
using System.Data.OleDb;
using System.Text.RegularExpressions;
using System.Web;

namespace dbf {
    class Program {
        static void Main(string[] args) {
            string 
                path = null,
                sqlQuery = null;

            // validate number of params
            if (args.Length != 4)
                UsageMessage();

            // get arguments
            for (var i = 0; i < args.Length; i++) {
                Regex r1 = new Regex( @"^-{2}(.*)" );
                Match match = r1.Match( args[ i ] );
                if ( match.Success ) {
                    string argName = match.Groups[1].ToString().ToLower();
                    if ( argName == "path" ) {
                        path = args[i + 1];
                        i++;
                    } else if ( argName == "query" ) {
                        sqlQuery = args[i + 1];
                        i++;
                    }
                }
            }

            // verify if arguments are correct
            if (path == null || sqlQuery == null)
                UsageMessage();

            // verify if base path is an existing directory
            if (!Directory.Exists(@path))
                ErrorMessage("the specified path isnt directory");

            // query execution
            DataTable rs = new DataTable();
            OleDbConnection db = new OleDbConnection(@"Provider=VFPOLEDB.1;Data Source=" + path + "; Collating Sequence=machine");
            db.Open();
            if (db.State == ConnectionState.Open) {
                try
                {
                    OleDbCommand query = new OleDbCommand(sqlQuery, db);
                    OleDbDataAdapter DA = new OleDbDataAdapter(query);
                    DA.Fill(rs);
                    db.Close();
                    string result = DataTableToJSONWithStringBuilder(rs);
                    Console.WriteLine(result);
                }
                catch ( OleDbException ex ) {
                    ErrorMessage(ex.Message);
                }
                
            }
            else
                ErrorMessage("database connection error");
        }

        /**
         * DataTable JSON encoder
         * Source http://www.c-sharpcorner.com/UploadFile/9bff34/3-ways-to-convert-datatable-to-json-string-in-Asp-Net-C-Sharp/
         * */
        public static string DataTableToJSONWithStringBuilder(DataTable table) {
            var JSONString = new StringBuilder();
            if (table.Rows.Count > 0)
            {
                JSONString.Append("[");
                for (int i = 0; i < table.Rows.Count; i++)
                {
                    JSONString.Append("{");
                    for (int j = 0; j < table.Columns.Count; j++)
                    {
                        if (j < table.Columns.Count - 1) {
                            JSONString.Append(HttpUtility.JavaScriptStringEncode(table.Columns[j].ColumnName.ToString(), true) + ":"
                            + HttpUtility.JavaScriptStringEncode(table.Rows[i][j].ToString().Trim(), true) + ","
                            );
                        } else if (j == table.Columns.Count - 1)
                            JSONString.Append(HttpUtility.JavaScriptStringEncode(table.Columns[j].ColumnName.ToString(), true) + ":"
                                + HttpUtility.JavaScriptStringEncode(table.Rows[i][j].ToString().Trim(), true)
                            );
                    }
                    if (i == table.Rows.Count - 1)
                        JSONString.Append("}");
                    else
                        JSONString.Append("},");
                }
                JSONString.Append("]");
            }
            // Set the output encoding to the default one used in Nodejs
            Console.OutputEncoding = System.Text.Encoding.UTF8;
            return JSONString.ToString();
        }

        public static void UsageMessage() {
            ErrorMessage("usage: dbf --path <dbf_base_path_files> --query <query_string>");
        }

        /**
         * error function
         * */
        public static void ErrorMessage(string message) {
            Console.Error.WriteLine( message );
            Environment.Exit(2);
        }
    }
}
