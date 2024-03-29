using System.Collections.Generic;
using Aunalytics.Sdk.Plugins;
using PluginSnowflake.DataContracts;

namespace PluginSnowflake.API.Replication
{
    public static partial class Replication 
    {
        public static ReplicationTable ConvertSchemaToReplicationTable(Schema schema, string schemaName,
            string tableName, bool shouldCreateSchema)
        {
            var table = new ReplicationTable
            {
                SchemaName = schemaName,
                TableName = tableName,
                Columns = new List<ReplicationColumn>(),
                ShouldCreateSchema = shouldCreateSchema
            };
            
            foreach (var property in schema.Properties)
            {
                var column = new ReplicationColumn
                {
                    ColumnName = property.Name,
                    DataType = string.IsNullOrWhiteSpace(property.TypeAtSource)? GetType(property.Type): property.TypeAtSource,
                    PrimaryKey = false
                };
                
                table.Columns.Add(column);
            }

            return table;
        }
        
        private static string GetType(PropertyType dataType)
        {
            switch (dataType)
            {
                case PropertyType.Datetime:
                    return "varchar(255)";
                case PropertyType.Date:
                    return "varchar(255)";
                case PropertyType.Time:
                    return "varchar(255)";
                case PropertyType.Integer:
                    return "int";
                case PropertyType.Decimal:
                    return "decimal";
                case PropertyType.Float:
                    return "double";
                case PropertyType.Bool:
                    return "boolean";
                case PropertyType.Blob:
                    return "varbinary";
                case PropertyType.String:
                    return "string";
                case PropertyType.Text:
                    return "text";
                default:
                    return "text";
            }
        }
    }
}