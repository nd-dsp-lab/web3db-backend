using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.ApiExplorer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Service.Core.APIBASE.StartupConfiguration.Swagger
{
    public static class AppExtensions
    {
        #region Methods
        public static IApplicationBuilder AddSwaggers(this IApplicationBuilder app, IApiVersionDescriptionProvider provider)
        {
            app.UseSwagger(c =>
            {
                c.SerializeAsV2 = true;
            });

            app.UseSwaggerUI(opt =>
            {
                opt.RoutePrefix = string.Empty;

                var basePath = string.IsNullOrEmpty(opt.RoutePrefix) ? "." : "..";

                foreach (var desc in provider.ApiVersionDescriptions)
                {
                    opt.SwaggerEndpoint($"{basePath}/swagger/{desc.GroupName}/swagger.json", $"v{desc.GroupName}");
                }
                opt.DocExpansion(Swashbuckle.AspNetCore.SwaggerUI.DocExpansion.List);

            });
            return app;
        }
        #endregion

    }
}
