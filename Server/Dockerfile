FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Shared/Shared.csproj Shared/
COPY Server/Server.csproj Server/
# необходимые библиотекти
RUN dotnet restore Server/Server.csproj

COPY Shared/ Shared/
COPY Server/ Server/
RUN dotnet publish Server/Server.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/publish .

EXPOSE 5377

ENTRYPOINT ["dotnet", "Server.dll"]