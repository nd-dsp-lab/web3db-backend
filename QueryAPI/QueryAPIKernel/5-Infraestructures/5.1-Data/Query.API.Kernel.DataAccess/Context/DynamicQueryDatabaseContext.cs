using Microsoft.EntityFrameworkCore;
using System.Reflection;
using Query.API.Kernel.DataAccess.Persistence.Extensions;
namespace Query.API.Kernel.DataAccess.Context
{
    public class DynamicQueryDatabaseContext : DbContext
    {
        #region Attributes
        public static string SCHEMA => "";
        #endregion
        #region Constructor
        public DynamicQueryDatabaseContext(DbContextOptions<DynamicQueryDatabaseContext> options) : base(options)
        {

        }
        #endregion
        #region Methods
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.AddDefaultSchema(SCHEMA);
            modelBuilder.ApplyConfigurationsFromAssembly(Assembly.GetExecutingAssembly());
            base.OnModelCreating(modelBuilder);

        }
        #endregion
    }
}
