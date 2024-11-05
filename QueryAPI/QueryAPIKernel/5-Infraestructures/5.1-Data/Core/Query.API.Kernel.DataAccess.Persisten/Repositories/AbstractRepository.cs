using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Query.API.Kernel.DataAccess.Persistence.Repositories.Interfaces;
using Query.API.Kernel.Domain.Core.Entities;
using System.Collections.Concurrent;

namespace Query.API.Kernel.DataAccess.Persistence.Repositories
{
    [Serializable]
    public abstract class AbstractRepository<T, TId> : IRepository<T, TId> where T : AbstractEntity<T, TId>
    {
        protected readonly ILogger<AbstractRepository<T, TId>> _logger;
        protected IServiceScopeFactory _scopeFactory;

        public AbstractRepository(ILogger<AbstractRepository<T, TId>> logger,
                                  IServiceScopeFactory scopeFactory)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
        }

        public async Task<T> FindIdAsync(TId id)
        {
            try
            {
                using (var scope = _scopeFactory.CreateScope())
                {
                    DbContext dbContext = scope.ServiceProvider.GetRequiredService<DbContext>();
                    DbSet<T> dataset = dbContext.Set<T>();

                    return await dataset.FirstOrDefaultAsync(x => x.Id.Equals(id));

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error when searching for ID {@id}");
                throw;
            }
        }
        public async Task<IEnumerable<T>> GetAll()
        {
            try
            {
                using (var scope = _scopeFactory.CreateScope())
                {
                    DbContext db = scope.ServiceProvider.GetRequiredService<DbContext>();
                    DbSet<T> dataset = db.Set<T>();

                    return await dataset.ToListAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error when searching for all entries");
                throw;
            }
        }
        public async Task<T> SaveAsync(T entity)
        {
            try
            {
                using (var scope = _scopeFactory.CreateScope())
                {
                    DbContext dbContext = scope.ServiceProvider.GetRequiredService<DbContext>();
                    DbSet<T> dataset = dbContext.Set<T>();

                    dataset.Add(entity);
                    await dbContext.SaveChangesAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error when saving entity");
                throw;
            }
            return entity;
        }
        public async Task<T> Add(T entity)
        {
            try
            {
                using (var scope = _scopeFactory.CreateScope())
                {
                    DbContext dbContext = scope.ServiceProvider.GetRequiredService<DbContext>();
                    DbSet<T> dataset = dbContext.Set<T>();

                    dataset.Add(entity);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error when saving entity");
            }
            return entity;
        }

        public async Task<IList<T>> SaveAsync(IList<T> entities)
        {
            ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();
            ConcurrentBag<T> entitiesRet = new ConcurrentBag<T>();
            ParallelOptions options = new ParallelOptions();
            options.MaxDegreeOfParallelism = 10;
            Parallel.ForEach(entities, options, entity =>
            {
                try
                {
                    entitiesRet.Add(UpdateAsync(entity).GetAwaiter().GetResult());

                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                }
            });
            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
            else
            {
                return entitiesRet.ToList();
            }
        }
        public async Task<T> SaveOrUpdateAsync(T entity, TId id)
        {
            try
            {
                using (var scope = _scopeFactory.CreateScope())
                {
                    DbContext dbContext = scope.ServiceProvider.GetRequiredService<DbContext>();
                    DbSet<T> dataset = dbContext.Set<T>();

                    var result = await dataset.SingleOrDefaultAsync(p => p.Id.Equals(id));
                    if (result == null || string.IsNullOrEmpty(id.ToString()) || id.Equals("0"))
                    {
                        await dataset.AddAsync(entity);
                    }
                    else
                    {
                        dbContext.Entry(result).CurrentValues.SetValues(entity);
                    }
                    await dbContext.SaveChangesAsync();

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error when saving/updating entry");
                throw;
            }
            return entity;
        }
        public async Task<IList<T>> SaveOrUpdateListAsync(IList<T> entities)
        {

            using (var scope = _scopeFactory.CreateScope())
            {
                DbContext dbContext = scope.ServiceProvider.GetRequiredService<DbContext>();
                DbSet<T> dataset = dbContext.Set<T>();

                using (var dbContextTransaction = dbContext.Database.BeginTransaction())
                {
                    List<Exception> exceptions = new List<Exception>();
                    List<T> returnEnt = new List<T>();
                    foreach (var entity in entities)
                    {
                        try
                        {
                            var result = dataset.SingleOrDefault(p => p.Id.Equals(entity.Id));
                            if (result == null || string.IsNullOrEmpty(entity.Id.ToString()) || entity.Id.Equals("0"))
                            {
                                dataset.Add(entity);
                                returnEnt.Add(entity);
                            }
                            else
                            {
                                dbContext.Entry(result).CurrentValues.SetValues(entity);
                                returnEnt.Add(entity);
                            }
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                        }

                    }

                    if (exceptions.Count > 0)
                    {
                        dbContextTransaction.Rollback();
                        return new List<T>();
                    }

                    int a = await dbContext.SaveChangesAsync();
                    dbContextTransaction.Commit();
                    return returnEnt;
                }
            }
        }

        public async Task<T> UpdateAsync(T entity)
        {
            try
            {
                using (var scope = _scopeFactory.CreateScope())
                {
                    DbContext dbContext = scope.ServiceProvider.GetRequiredService<DbContext>();
                    DbSet<T> dataset = dbContext.Set<T>();

                    dataset.Update(entity);

                    await dbContext.SaveChangesAsync();

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error when updating entry");
                throw;
            }
            return entity;
        }
        public async Task<IList<T>> UpdateListAsync(IList<T> entities)
        {
            ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();
            ConcurrentBag<T> entitiesRet = new ConcurrentBag<T>();
            ParallelOptions options = new ParallelOptions();
            options.MaxDegreeOfParallelism = 10;
            Parallel.ForEach(entities, options, entity =>
            {
                try
                {
                    entitiesRet.Add(UpdateAsync(entity).GetAwaiter().GetResult());
                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                }
            });
            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
            else
            {
                return entitiesRet.ToList();
            }
        }
        public async Task<bool> DeleteAsync(T entity)
        {
            try
            {
                using (var scope = _scopeFactory.CreateScope())
                {
                    DbContext dbContext = scope.ServiceProvider.GetRequiredService<DbContext>();
                    DbSet<T> dataset = dbContext.Set<T>();

                    var result = await dataset.SingleOrDefaultAsync(p => p.Id.Equals(entity.Id));
                    if (result == null)
                    {
                        return false;
                    }
                    dataset.Remove(result);
                    await dbContext.SaveChangesAsync();
                    return true;

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error when deleting entity");
                throw ex;
            }
        }
    }
}
