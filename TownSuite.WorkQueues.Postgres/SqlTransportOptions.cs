namespace TownSuite.WorkQueues.Postgres
{
    public class SqlTransportOptions : BatchOptions
    {
        /// <summary>
        /// Used to connect to the PostgreSQL database for message transport.
        /// </summary>
        public string ConnectionString { get; set; } = string.Empty;

        /// <summary>
        /// Is the Schema that contains the message transport tables.
        /// </summary>
        public string Schema { get; set; } = "transport";

        /// <summary>
        /// Used to run migrations and manage the database schema.
        /// Defaults to <see cref="ConnectionString"/> when not explicitly set.
        /// </summary>
        public string AdminConnectionString
        {
            get => string.IsNullOrEmpty(_adminConnectionString) ? ConnectionString : _adminConnectionString;
            set => _adminConnectionString = value;
        }
        private string _adminConnectionString = string.Empty;
    }
}
