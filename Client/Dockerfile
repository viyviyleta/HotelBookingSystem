FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Shared/Shared.csproj Shared/
COPY Client/Client.csproj Client/
RUN dotnet restore Client/Client.csproj

COPY Shared/ Shared/
COPY Client/ Client/
RUN dotnet publish Client/Client.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Client.dll"]