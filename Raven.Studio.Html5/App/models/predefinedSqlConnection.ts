class predefinedSqlConnection {
    name=ko.observable<string>();
    factoryName = ko.observable<string>();
    connectionString = ko.observable<string>();

    constructor(dto: predefinedSqlConnectionDto) {
        this.name(dto.Name);
        this.factoryName(dto.FactoryName);
        this.connectionString(dto.ConnectionString);
    }

    static empty(): predefinedSqlConnection {
        return new predefinedSqlConnection({
            Name:"",
            FactoryName: "",
            ConnectionString:""
        });
    }

    toDto(): predefinedSqlConnectionDto {
        return {
            Name:this.name(),
            FactoryName: this.factoryName(),
            ConnectionString: this.connectionString()
        }
    }
}

export =predefinedSqlConnection;