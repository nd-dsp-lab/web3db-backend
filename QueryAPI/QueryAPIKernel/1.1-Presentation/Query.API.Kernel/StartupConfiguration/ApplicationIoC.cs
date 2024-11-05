using Query.API.Kernel.Application.Query;
using Query.API.Kernel.Application.Query.Actions;
using Query.API.Kernel.Application.Query.Interfaces;
using Query.API.Kernel.Application.Query.Interfaces.Actions;

namespace Query.API.Kernel.StartupConfiguration
{
    public static class ApplicationIoC
    {
        public static void AddQueryApplication(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton<ISendQueryAction, SendQueryAction>();

            #region Http
            services.AddHttpClient<ISendQueryAction, SendQueryAction>(client =>
            {
                client.BaseAddress = new Uri(configuration["Query:Url"]);
                client.DefaultRequestHeaders.Add("allow-cache", "true");

            });
            #endregion
            #region Actions
            #endregion
            #region Application
            services.AddSingleton<IQueryApplication, QueryApplication>();

            #endregion
        }
    }
}
