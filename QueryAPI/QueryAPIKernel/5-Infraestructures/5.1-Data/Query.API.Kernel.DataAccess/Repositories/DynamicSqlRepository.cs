using Microsoft.EntityFrameworkCore;
using Query.API.Kernel.DataAccess.Context;
using Query.API.Kernel.DataAccess.Repositories.Interfaces;
using Query.API.Kernel.Domain.Tables;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;


namespace Query.API.Kernel.DataAccess.Repositories
{
    public class DynamicSqlRepository : IDynamicSqlRepository
    {
        private readonly DynamicQueryDatabaseContext _context;

        public DynamicSqlRepository(DynamicQueryDatabaseContext context)
        {
            _context = context;
        }

        public async Task<TableData> ExecuteQueryAsync(string userQuery, string schema, string tableName)
        {
            if (string.IsNullOrWhiteSpace(userQuery))
            {
                throw new ArgumentException("Query cannot be empty or null.");
            }

            // Modifying the query to ensure it targets the specified schema and table.
            var modifiedQuery = $"{userQuery} FROM \"{schema}\".\"{tableName}\"";

            if (modifiedQuery.TrimStart().ToUpper().StartsWith("SELECT"))
            {
                var entities = await ExecuteSelectQueryAsync(modifiedQuery);
                var tableData = new TableData();
                if (entities.Any())
                {
                    tableData.Columns = entities.First().ToDictionary(kvp => kvp.Key, kvp => kvp.Key.GetType().Name);
                    tableData.Rows = entities.Select(e => new DatabaseRow { Fields = e }).ToList();
                }
                return tableData;
            }
            else
            {
                int affectedRows = await ExecuteCommandAsync(modifiedQuery);
                var tableData = new TableData
                {
                    Columns = new Dictionary<string, string> { { "AffectedRows", typeof(int).Name } },
                    Rows = new List<DatabaseRow>
            {
                new DatabaseRow { Fields = new Dictionary<string, object> { { "AffectedRows", affectedRows } } }
            }
                };
                return tableData;
            }
        }

        private async Task<List<Dictionary<string, object>>> ExecuteSelectQueryAsync(string sql)
        {
            using var command = _context.Database.GetDbConnection().CreateCommand();
            command.CommandText = sql;

            _context.Database.OpenConnection();

            using var result = await command.ExecuteReaderAsync();
            var entities = new List<Dictionary<string, object>>();

            while (result.Read())
            {
                var entity = new Dictionary<string, object>();
                for (int i = 0; i < result.FieldCount; i++)
                {
                    entity[result.GetName(i)] = result.GetValue(i);
                }
                entities.Add(entity);
            }

            return entities;
        }

        private async Task<int> ExecuteCommandAsync(string sql)
        {
            using var command = _context.Database.GetDbConnection().CreateCommand();
            command.CommandText = sql;

            _context.Database.OpenConnection();

            return await command.ExecuteNonQueryAsync();
        }

        public async Task<List<string>> FetchAllSchemasAsync()
        {
            var results = await ExecuteSelectQueryAsync("SELECT schema_name FROM information_schema.schemata");
            return results.Select(dict => dict["schema_name"].ToString()).ToList();
        }

        public async Task<List<string>> FetchTableNamesForSchemaAsync(string schema)
        {
            var results = await ExecuteSelectQueryAsync($"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'");
            return results.Select(dict => dict["table_name"].ToString()).ToList();
        }

        public async Task<TableData> FetchFullTableAsync(string schema, string tableName)
        {
            var tableDataQuery = $"SELECT * FROM \"{schema}\".\"{tableName}\"";
            var entities = await ExecuteSelectQueryAsync(tableDataQuery);

            var tableData = new TableData();
            if (entities.Any())
            {
                tableData.Columns = entities.First().ToDictionary(kvp => kvp.Key, kvp => kvp.Key.GetType().Name);
                tableData.Rows = entities.Select(e => new DatabaseRow { Fields = e }).ToList();
            }
            return tableData;
        }
    }
}
