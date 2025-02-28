using System.Collections.Generic;
using System.Threading.Tasks;
using PluginSnowflake.API.Factory;
using PluginSnowflake.DataContracts;

namespace PluginSnowflake.API.Write
{
    public static partial class Write
    {
        private const string SchemaName = "PROCEDURE_SCHEMA";
        private const string RoutineName = "PROCEDURE_NAME";

        private static string GetAllStoredProceduresQuery = @"select PROCEDURE_SCHEMA, PROCEDURE_NAME from INFORMATION_SCHEMA.PROCEDURES";

        public static async Task<List<WriteStoredProcedure>> GetAllStoredProceduresAsync(IConnectionFactory connFactory)
        {
            var storedProcedures = new List<WriteStoredProcedure>();
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(GetAllStoredProceduresQuery, conn);
                var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    var storedProcedure = new WriteStoredProcedure
                    {
                        SchemaName = reader.GetValueById(SchemaName).ToString(),
                        RoutineName = reader.GetValueById(RoutineName).ToString(),
                    };

                    storedProcedures.Add(storedProcedure);
                }

                return storedProcedures;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}