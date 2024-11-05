# Base image for the application runtime
FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

# Image used to build the application
FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["1.1-Presentation/Query.API.Kernel/Query.API.Kernel.csproj", "1.1-Presentation/Query.API.Kernel/"]
RUN dotnet restore "1.1-Presentation/Query.API.Kernel/Query.API.Kernel.csproj"
COPY . .
WORKDIR "/src/1.1-Presentation/Query.API.Kernel"
RUN dotnet build "Query.API.Kernel.csproj" -c Release -o /app/build

# Generate dev certificate
RUN dotnet dev-certs https -ep /app/aspnetapp.pfx -p password

# Publish the application
FROM build AS publish
RUN dotnet publish "Query.API.Kernel.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Final image for running the application
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
COPY --from=build /app/aspnetapp.pfx /https/aspnetapp.pfx

# Environment variables to configure Kestrel for HTTPS
ENV ASPNETCORE_URLS="https://+:443;http://+:80"
ENV ASPNETCORE_HTTPS_PORT=443
ENV ASPNETCORE_Kestrel__Certificates__Default__Path=/https/aspnetapp.pfx
ENV ASPNETCORE_Kestrel__Certificates__Default__Password=password
ENV ASPNETCORE_ENVIRONMENT=Development

ENTRYPOINT ["dotnet", "Query.API.Kernel.dll"]