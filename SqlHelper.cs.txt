using DAL.Helpers.Models;
using DAL.Models;
using DAL.Extentions;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL.Helpers
{
    internal static class SqlHelper
    {
        public static ConnectionModel ConnectionSettings { get; set; }

        public static IEnumerable<T> ExecuteStoredProcedure<T>(string storedProc, List<SqlParameter> parameters, Func<DbDataReader, T> extractResult)
        {
            List<T> Result = new List<T>();

            using (var connection = new SqlConnection(ConnectionSettings.ConnectionString()))
            {
                connection.Open();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    using (SqlCommand command = new SqlCommand(storedProc, connection))
                    {

                        command.CommandTimeout = 120;
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Transaction = transaction;

                        foreach (var sqlParameter in parameters)
                        {
                            command.Parameters.Add(sqlParameter);
                        }

                        try
                        {
                            using (DbDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    Result.Add(extractResult(reader));
                                }
                                reader.Close();
                            }

                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            Exception exception = new Exception($"{storedProc} Failed, see inner exception for details", ex);
                            throw exception;
                        }
                    }
                }
                connection.Close();
            }
            return Result;
        }

        public static bool ExecuteStoredProcedure(string storedProc, List<SqlParameter> parameters)
        {
            using (var connection = new SqlConnection(ConnectionSettings.ConnectionString()))
            {
                connection.Open();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    using (SqlCommand command = new SqlCommand(storedProc, connection))
                    {
                        command.CommandTimeout = 120;
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Transaction = transaction;

                        foreach (var sqlParameter in parameters)
                        {
                            command.Parameters.Add(sqlParameter);
                        }

                        try
                        {
                            command.ExecuteNonQuery();
                            transaction.Commit();
                        }
                        catch (SqlException ex)
                        {
                            transaction.Rollback();
                            Exception exception = new Exception($"{storedProc} Failed, see inner exception for details", ex);
                            throw exception;
                        }
                    }
                }
                connection.Close();
            }

            return true;
        }
        #region Async Implementation
        /// <summary>
        /// Used to verify DB connectivity
        /// </summary>
        /// <returns>boolean</returns>
        public async static Task<bool> CheckConnectionAsync(ConnectionModel conModel)
        {
            var conStr = conModel.ConnectionString();
            if (string.IsNullOrEmpty(conStr))
            {
                throw new Exception("Connection string cannot be empty");
            }
            else
            {
                try
                {
                    using (SqlConnection connection = new SqlConnection(conStr))
                    {
                        await connection.OpenAsync();//.ConfigureAwait(false);
                        Console.WriteLine($"Connection to {conModel.DataSource}/{conModel.DbName} successful");
                        connection.Close();
                    }
                }
                catch (Exception ex)
                {

                    throw;
                }
            }

            return true;
        }

        public async static Task<bool> CheckConnectionAsync()
        {
            var conStr = ConnectionSettings.ConnectionString();
            if (string.IsNullOrEmpty(conStr))
            {
                throw new Exception("Connection string cannot be empty");
            }
            else
            {
                using (SqlConnection connection = new SqlConnection(conStr))
                {
                    await connection.OpenAsync();//.ConfigureAwait(false);
                    connection.Close();
                }
            }

            return true;
        }

        /// <summary>
        /// Executes the given parameter and returns a list of the specified type
        /// </summary>
        /// <typeparam name="T">The result type</typeparam>
        /// <param name="storedProc">The SP to execute</param>
        /// <param name="parameters">A collection of parameters for the SP</param>
        /// <param name="extractResult">The function used to extract the specified type from the DBDataReader</param>
        /// <returns></returns>
        public async static Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string storedProc, List<SqlParameter> parameters, Func<DbDataReader, T> extractResult)
        {
            List<T> Result = new List<T>();

            using (var connection = new SqlConnection(ConnectionSettings.ConnectionString()))
            {
                await connection.OpenAsync();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    using (SqlCommand command = new SqlCommand(storedProc, connection))
                    {

                        command.CommandTimeout = 120;
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Transaction = transaction;

                        foreach (var sqlParameter in parameters)
                        {
                            command.Parameters.Add(sqlParameter);
                        }

                        try
                        {
                            using (DbDataReader reader = await command.ExecuteReaderAsync())
                            {
                                while (await reader.ReadAsync())
                                {
                                    Result.Add(extractResult(reader));
                                }
                                reader.Close();
                            }

                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            Exception exception = new Exception($"{storedProc} Failed, see inner exception for details", ex);
                            throw exception;
                        }
                    }
                }
                connection.Close();
            }
            return Result;
        }

        public async static Task<bool> ExecuteStoredProcedureAsync(string storedProc, List<SqlParameter> parameters)
        {
            using (var connection = new SqlConnection(ConnectionSettings.ConnectionString()))
            {
                await connection.OpenAsync();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    using (SqlCommand command = new SqlCommand(storedProc, connection))
                    {
                        command.CommandTimeout = 120;
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Transaction = transaction;

                        foreach (var sqlParameter in parameters)
                        {
                            command.Parameters.Add(sqlParameter);
                        }

                        try
                        {
                            await command.ExecuteNonQueryAsync();
                            transaction.Commit();
                        }
                        catch (SqlException ex)
                        {
                            transaction.Rollback();
                            Exception exception = new Exception($"{storedProc} Failed, see inner exception for details", ex);
                            throw exception;
                        }
                    }
                }
                connection.Close();
            }

            return true;
        }

        /// <summary>
        /// Selects rows from specified table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="criteria">Results will be ordered by the first column in the column list</param>
        /// <param name="extractResult"></param>
        /// <returns></returns>
        public async static Task<IEnumerable<T>> SelectRowsFromTable<T>(SelectionCriteria criteria, Func<DbDataReader, T> extractResult)
        {
            List<T> Result = new List<T>();

            using (var connection = new SqlConnection(ConnectionSettings.ConnectionString()))
            {
                await connection.OpenAsync();
                // get the source data
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    //Get data
                    using (SqlCommand command = new SqlCommand($"SELECT {ConvertListToCommaDelimitedString(criteria.Columns)} FROM {criteria.Table} {BuildWhereFromCriteria(criteria)} ORDER BY {criteria.Columns.FirstOrDefault()} OFFSET {criteria.Skip} ROWS FETCH NEXT {criteria.Take} ROWS ONLY; ", connection, transaction))
                    {
                        try
                        {
                            using (DbDataReader reader = await command.ExecuteReaderAsync())
                            {
                                while (await reader.ReadAsync())
                                {
                                    Result.Add(extractResult(reader));
                                }
                                reader.Close();
                            }
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            Exception exception = new Exception($"SqlHelper Select Failed, see inner exception for details", ex);
                            throw exception;
                        }
                    }
                }
                connection.Close();
            }
            return Result;
        } 
        #endregion

        /// <summary>
        /// Convert List of Scalars to comma delimited string
        /// </summary>
        /// <typeparam name="T">Scalar Type</typeparam>
        /// <param name="list">List of Scalars</param>
        /// <returns>Comma delimited string</returns>
        public static string ConvertListToCommaDelimitedString<T>(IEnumerable<T> list)
        {
            var sb = new StringBuilder();

            if (list == null)
                return sb.ToString();

            //Build parameter
            list.Where(x => x != null).ToList().ForEach(x =>
            {
                sb.Append($"{x.ToString()},");
            });

            return sb.ToString().TrimEnd(',');
        }

        /// <summary>
        /// Convert Key Value Pair List to comma delimited string
        /// </summary>
        /// <typeparam name="T1">Scalar Type</typeparam>
        /// <typeparam name="T2">Scalar Type</typeparam>
        /// <param name="list">Key Value Pair List</param>
        /// <returns>Comma delimited string</returns>
        public static string ConvertKeyValuePairListToCommaDelimitedString<T1, T2>(IEnumerable<KeyValuePair<T1, T2>> list)
        {
            var sb = new StringBuilder();

            if (list == null || !list.Any())
                return sb.ToString();

            //Build parameter
            list.ToList().ForEach(x =>
            {
                if (x.Value != null && !string.IsNullOrWhiteSpace(x.Value.ToString()))
                {
                    sb.Append($"{x.Key};{RemoveCommas(x.Value.ToString())},");
                }
            });

            return sb.ToString().TrimEnd(',');
        }

        public static int ReadAsInt(DbDataReader reader, int index)
        {
            int value = 0;
            if (!reader.IsDBNull(index))
            {
                //Caters for both Int32 and Int16 situations
                int.TryParse(reader[index].ToString(), out value);
            }

            return value;
        }

        public static string ReadAsString(DbDataReader reader, int index)
        {
            try
            {
                return !reader.IsDBNull(index) ? reader.GetString(index) : string.Empty;
            }
            catch (Exception ex)
            {

                throw;
            }
        }

        public static string ReadAsDateString(DbDataReader reader, int index)
        {
            return !reader.IsDBNull(index) ? reader.GetDateTime(index).ToString("yyyy-MM-dd HH:mm:ss.fff") : string.Empty;
        }

        public static DateTime ReadAsDate(DbDataReader reader, int index)
        {
            return !reader.IsDBNull(index) ? reader.GetDateTime(index) : DateTime.MinValue;
        }

        public static bool ReadHasValue(DbDataReader reader, int index)
        {
            object value = !reader.IsDBNull(index) ? reader.GetValue(index) : null;

            return value != null;
        }

        public static bool ReadAsBool(DbDataReader reader, int index)
        {
            bool value = false;
            if (ReadHasValue(reader, index))
            {
                bool.TryParse(reader[index].ToString(), out value);
            }

            return value;
        }

        public static byte[] ReadAsByte(DbDataReader reader, int index)
        {
            return !reader.IsDBNull(index) ? (byte[])reader.GetValue(index) : null;
        }

        public static object BuildParameters<T>(IEnumerable<T> values)
        {
            if (values == null)
                return DBNull.Value;

            return SqlHelper.ConvertListToCommaDelimitedString<T>(values.ToList());
        }

        public static object BuildParameter<T>(IEnumerable<T> values)
        {
            if (values == null)
                return DBNull.Value;

            var value = (T)values.FirstOrDefault();

            //Rather return DBNull instead of empty string
            if (value is string && string.IsNullOrWhiteSpace(value.ToString()))
                return DBNull.Value;

            return value;
        }

        public static object BuildParameter<T>(T value)
        {
            if (value == null)
                return DBNull.Value;

            return (T)value;
        }

        private static string BuildWhereFromCriteria(QueryCriteria criteria)
        {
            if (criteria.WherePredicate.Count() == 0)
                return string.Empty;

            var sb = new StringBuilder("WHERE ");

            criteria.WherePredicate.ToList().ForEach(x =>
            {
                sb.Append($"{x.Column} {x.Opperator} {x.Value}");
                if (x != criteria.WherePredicate.Last())
                    sb.Append(" AND ");
            });

            return sb.ToString();
        }

        private static string RemoveCommas(string str)
        {
            if (string.IsNullOrWhiteSpace(str))
                return str;

            return str.Replace(",", "");
        }
    }
}
