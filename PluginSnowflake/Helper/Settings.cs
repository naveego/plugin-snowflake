using System;

namespace PluginSnowflake.Helper
{
    public class Settings
    {
        public string Account { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }
        public string Warehouse { get; set; }

        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(Account))
            {
                throw new Exception("The Account property must be set");
            }
            
            if (String.IsNullOrEmpty(Database))
            {
                throw new Exception("The Database property must be set");
            }

            if (String.IsNullOrEmpty(User))
            {
                throw new Exception("The User property must be set");
            }
            
            if (String.IsNullOrEmpty(Password))
            {
                throw new Exception("The Password property must be set");
            }
            
            if (String.IsNullOrEmpty(Warehouse))
            {
                throw new Exception("The Warehouse property must be set");
            }
        }

        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString()
        {
            return $"account={Account};user={User};password={Password};db={Database};";
        }
        
        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString(string database)
        {
            return $"account={Account};user={User};password={Password};db={database};warehouse={Warehouse};";
        }
    }
}