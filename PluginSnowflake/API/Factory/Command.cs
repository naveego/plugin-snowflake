using System.Threading.Tasks;
using Snowflake.Data.Client;

namespace PluginSnowflake.API.Factory
{
    public class Command : ICommand
    {
        private readonly SnowflakeDbCommand _cmd;

        public Command()
        {
            _cmd = new SnowflakeDbCommand();
        }

        public Command(string commandText)
        {
            _cmd = new SnowflakeDbCommand();
            _cmd.CommandText = commandText;
        }

        public Command(string commandText, IConnection conn)
        {
            _cmd = new SnowflakeDbCommand();
            _cmd.CommandText = commandText;
            _cmd.Connection = (SnowflakeDbConnection) conn.GetConnection();
        }

        public void SetConnection(IConnection conn)
        {
            _cmd.Connection = (SnowflakeDbConnection) conn.GetConnection();
        }

        public void SetCommandText(string commandText)
        {
            _cmd.CommandText = commandText;
        }

        public void AddParameter(string name, object value)
        {
            var param = _cmd.CreateParameter();
            param.ParameterName = name;
            param.Value = value;
            param.DbType = System.Data.DbType.Object;
            _cmd.Parameters.Add(param);
        }

        public async Task<IReader> ExecuteReaderAsync()
        {
            return new Reader(await _cmd.ExecuteReaderAsync());
        }

        public async Task<int> ExecuteNonQueryAsync()
        {
            return await _cmd.ExecuteNonQueryAsync();
        }
    }
}