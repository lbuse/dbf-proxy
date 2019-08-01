using System;
using System.IO;

using System.Data;
using System.Data.OleDb;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using Newtonsoft.Json;

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
                string sqlCommandType = "";
                if (sqlQuery.Length > 0)
                {
                    string[] tokens = sqlQuery.Split(' ');
                    sqlCommandType = tokens[0];
                }

                string result = "[]";
                string[] updateCommands = { "UPDATE", "INSERT", "DELETE" };

                try
                {
                    OleDbCommand query = new OleDbCommand(sqlQuery, db);

                    if (Array.Exists(updateCommands, element => element == sqlCommandType.ToUpper())) {
                        result = "[{\"rows\":" + query.ExecuteNonQuery() + "}]";
                    } else
                    {
                        OleDbDataAdapter DA = new OleDbDataAdapter(query);
                        DA.Fill(rs);
                        db.Close();
                        result = DataTableToJson(rs);
                    }

                    Console.WriteLine(result);
                }
                catch (OleDbException ex)
                {
                    ErrorMessage(ex.Message);
                }
                finally {
                    if(db.State == ConnectionState.Open)
                        db.Close();
                }
                
            }
            else
                ErrorMessage("database connection error");
        }

        public static string DataTableToJson(DataTable table) {
  
            List<Dictionary<string, object>> parentRow = new List<Dictionary<string, object>>();
            Dictionary<string, object> childRow;
            foreach (DataRow row in table.Rows)
            {
                childRow = new Dictionary<string, object>();
                foreach (DataColumn col in table.Columns)
                {
                    var value = row[col].GetType() == typeof(string)
                            ? row[col].ToString().Trim()
                            : row[col];

                    childRow.Add(col.ColumnName, value);
                }
                parentRow.Add(childRow);
            }
            return JsonConvert.SerializeObject(parentRow);
            
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
