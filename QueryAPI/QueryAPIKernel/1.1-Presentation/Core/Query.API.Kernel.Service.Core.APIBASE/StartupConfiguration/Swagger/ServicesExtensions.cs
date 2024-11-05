using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Swashbuckle.AspNetCore.SwaggerGen;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Service.Core.APIBASE.StartupConfiguration.Swagger
{
    public static class ServicesExtensions
    {
        #region Methods
        public static IServiceCollection AddSwaggers(this IServiceCollection service)
        {
            service.AddTransient<IConfigureOptions<SwaggerGenOptions>, ConfigureSwaggerOptions>();
            service.AddSwaggerGen(opt =>
            {
                opt.EnableAnnotations();
                opt.CustomSchemaIds(type => type.FullName);
            });

            return service;

        }
        #endregion

    }
}
