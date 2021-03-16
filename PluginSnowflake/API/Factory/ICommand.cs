using System.Threading.Tasks;

namespace PluginSnowflake.API.Factory
{
    public interface ICommand
    {
        void SetConnection(IConnection conn);
        void SetCommandText(string commandText);
        void AddParameter(string name, object value);
        Task<IReader> ExecuteReaderAsync();
        Task<int> ExecuteNonQueryAsync();
    }
}