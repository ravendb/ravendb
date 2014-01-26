class spatialIndexField {
    name = ko.observable<string>();
    type = ko.observable<string>();
    strategy = ko.observable<string>();
    circleRadiusUnits = ko.observable<string>();
    maxTreeLevel = ko.observable<number>();
    minX = ko.observable<number>();
    maxX = ko.observable<number>();
    minY = ko.observable<number>();
    maxY = ko.observable<number>();
    units = ko.observable<string>();
    precision: KnockoutComputed<string>;
    availableStrategies = ko.observableArray<string>();
    canSpecifyUnits: KnockoutComputed<boolean>;
    canSpecifyTreeLevel: KnockoutComputed<boolean>;
    canSpecifyCoordinates: KnockoutComputed<boolean>;

    private static strategyGeo = "GeohashPrefixTree";
    private static strategyBounding = "BoundingBox";
    private static strategyQuad = "QuadPrefixTree";
    private static typeGeo = "Geography";
    private static typeCart = "Cartesian";

    constructor(fieldName: string, dto: spatialIndexFieldDto) {
        this.name(fieldName);
        this.type(dto.Type);
        this.strategy(dto.Strategy);
        this.maxTreeLevel(dto.MaxTreeLevel);
        this.minX(dto.MinX);
        this.maxX(dto.MaxX);
        this.minY(dto.MinY);
        this.maxY(dto.MaxY);
        this.units(dto.Units);

        this.availableStrategies(this.getAvailableStrategies());
        this.canSpecifyUnits = ko.computed(() => this.type() === spatialIndexField.typeGeo);
        this.canSpecifyTreeLevel = ko.computed(() => this.strategy() !== spatialIndexField.strategyBounding);
        this.precision = ko.computed(() => this.getPrecisionString());
        this.canSpecifyCoordinates = ko.computed(() => this.type() === spatialIndexField.typeCart);
        this.type.subscribe(newType => this.resetCoordinates());
        this.type.subscribe(() => this.availableStrategies(this.getAvailableStrategies()));
        this.strategy.subscribe(newStrategy => this.updateMaxTreeLevelFromStrategy(newStrategy));
    }

    toDto(): spatialIndexFieldDto {
        return {
            Type: this.type(),
            Strategy: this.strategy(),
            MaxTreeLevel: this.maxTreeLevel(),
            MinX: this.minX(),
            MaxX: this.maxX(),
            MinY: this.minY(),
            MaxY: this.maxY(),
            Units: this.units()
        };
    }

    static empty(): spatialIndexField {
        var dto: spatialIndexFieldDto = {
            Type: spatialIndexField.typeGeo,
            MaxTreeLevel: 9,
            MinX: -180,
            MaxX: 180,
            MinY: -90,
            MaxY: 90,
            Strategy: spatialIndexField.strategyGeo,
            Units: "Kilometers"
        };
        return new spatialIndexField("", dto);
    }

    private resetCoordinates() {
        this.minX(-180);
        this.maxX(180);
        this.minY(-90);
        this.maxY(90);
    }

    private getPrecisionString(): string {
        var strategy = this.strategy();
        var minX = this.minX();
        var maxX = this.maxX();
        var minY = this.minY();
        var maxY = this.maxY();
        var maxTreeLevel = this.maxTreeLevel();
        var type = this.type();
        var units = this.units();

        if (strategy === spatialIndexField.strategyBounding) {
            return "";
        }

        var x = maxX - minX;
        var y = maxY - minY;
        for (var i = 0; i < maxTreeLevel; i++) {
            if (strategy === spatialIndexField.strategyGeo) {
                if (i % 2 == 0) {
                    x /= 8;
                    y /= 4;
                }
                else {
                    x /= 4;
                    y /= 8;
                }
            }
            else if (strategy === spatialIndexField.strategyQuad) {
                x /= 2;
                y /= 2;
            }
        }

        if (type === spatialIndexField.typeGeo) {
            var earthMeanRadiusKm = 6371.0087714;
            var milesToKm = 1.60934;

			var factor = (earthMeanRadiusKm * Math.PI * 2) / 360;
            x = x * factor;
            y = y * factor;
            if (units === "Miles") {
                x /= milesToKm;
                y /= milesToKm;
            }

            return "Precision at equator; X: " + x.toFixed(6) + ", Y: " + y.toFixed(6) + " " + units.toLowerCase();
        } else {
            return "Precision; X: " + x.toFixed(6) + ", Y: " + y.toFixed(6);
        }
    }

    private getAvailableStrategies(): string[] {
        if (this.type() === spatialIndexField.typeGeo) {
            return [spatialIndexField.strategyGeo, spatialIndexField.strategyQuad, spatialIndexField.strategyBounding];
        } else {
            return [spatialIndexField.strategyQuad, spatialIndexField.strategyBounding];
        }
    }

    private updateMaxTreeLevelFromStrategy(strategy: string) {
        if (strategy === spatialIndexField.strategyGeo) {
            this.maxTreeLevel(9);
        } else if (strategy === spatialIndexField.strategyQuad) {
            this.maxTreeLevel(23);
        }
    }
}

export = spatialIndexField; 