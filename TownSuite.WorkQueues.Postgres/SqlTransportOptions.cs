using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        /// </summary>
        public string AdminConnectionString { get; set; } = string.Empty;
    }
}
