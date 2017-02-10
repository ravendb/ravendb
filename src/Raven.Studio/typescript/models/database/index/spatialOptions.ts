/// <reference path="../../../../typings/tsd.d.ts"/>

class spatialOptions {
    maxTreeLevel = ko.observable<number>();
    minX = ko.observable<number>();
    maxX = ko.observable<number>();
    minY = ko.observable<number>();
    maxY = ko.observable<number>();
    strategy = ko.observable<Raven.Client.Indexing.SpatialSearchStrategy>();
    type = ko.observable<Raven.Client.Indexing.SpatialFieldType>();
    units = ko.observable<Raven.Client.Indexing.SpatialUnits>();
    
    precision: KnockoutComputed<string>;
    availableStrategies = ko.observableArray<string>();
    canSpecifyUnits: KnockoutComputed<boolean>;
    canSpecifyTreeLevel: KnockoutComputed<boolean>;
    canSpecifyCoordinates: KnockoutComputed<boolean>;

    private static readonly strategyGeo = "GeohashPrefixTree" as Raven.Client.Indexing.SpatialSearchStrategy;
    private static readonly strategyBounding = "BoundingBox" as Raven.Client.Indexing.SpatialSearchStrategy;
    private static readonly strategyQuad = "QuadPrefixTree" as Raven.Client.Indexing.SpatialSearchStrategy;
    private static readonly typeGeo = "Geography" as Raven.Client.Indexing.SpatialFieldType;
    private static readonly typeCart = "Cartesian" as Raven.Client.Indexing.SpatialFieldType;

    constructor(dto: Raven.Client.Indexing.SpatialOptions) {
        this.type(dto.Type);
        this.strategy(dto.Strategy);
        this.maxTreeLevel(dto.MaxTreeLevel);
        this.minX(dto.MinX);
        this.maxX(dto.MaxX);
        this.minY(dto.MinY);
        this.maxY(dto.MaxY);
        this.units(dto.Units);

        this.availableStrategies(this.getAvailableStrategies());
        this.canSpecifyUnits = ko.pureComputed(() => this.type() === spatialOptions.typeGeo);
        this.canSpecifyTreeLevel = ko.pureComputed(() => this.strategy() !== spatialOptions.strategyBounding);
        this.precision = ko.pureComputed(() => this.getPrecisionString());
        this.canSpecifyCoordinates = ko.pureComputed(() => this.type() === spatialOptions.typeCart);
        this.type.subscribe(newType => {
            this.resetCoordinates();
            this.availableStrategies(this.getAvailableStrategies());
        });
        this.strategy.subscribe(newStrategy => this.updateMaxTreeLevelFromStrategy(newStrategy));
    }

    toDto(): Raven.Client.Indexing.SpatialOptions {
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

    static empty(): spatialOptions {
        const dto: Raven.Client.Indexing.SpatialOptions = {
            Type: spatialOptions.typeGeo,
            MaxTreeLevel: 9,
            MinX: -180,
            MaxX: 180,
            MinY: -90,
            MaxY: 90,
            Strategy: spatialOptions.strategyGeo,
            Units: "Kilometers"
        };
        return new spatialOptions(dto);
    }

    private resetCoordinates() {
        this.minX(-180);
        this.maxX(180);
        this.minY(-90);
        this.maxY(90);
    }

    private getPrecisionString(): string {
        const strategy = this.strategy();
        const minX = this.minX();
        const maxX = this.maxX();
        const minY = this.minY();
        const maxY = this.maxY();
        const maxTreeLevel = this.maxTreeLevel();
        const type = this.type();
        const units = this.units();

        if (strategy === spatialOptions.strategyBounding) {
            return "";
        }

        let x = maxX - minX;
        let y = maxY - minY;
        for (let i = 0; i < maxTreeLevel; i++) {
            if (strategy === spatialOptions.strategyGeo) {
                if (i % 2 == 0) {
                    x /= 8;
                    y /= 4;
                }
                else {
                    x /= 4;
                    y /= 8;
                }
            }
            else if (strategy === spatialOptions.strategyQuad) {
                x /= 2;
                y /= 2;
            }
        }

        if (type === spatialOptions.typeGeo) {
            const earthMeanRadiusKm = 6371.0087714;
            const milesToKm = 1.60934;

            let factor = (earthMeanRadiusKm * Math.PI * 2) / 360;
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

    private getAvailableStrategies(): Raven.Client.Indexing.SpatialSearchStrategy[] {
        if (this.type() === spatialOptions.typeGeo) {
            return [spatialOptions.strategyGeo, spatialOptions.strategyQuad, spatialOptions.strategyBounding];
        } else {
            return [spatialOptions.strategyQuad, spatialOptions.strategyBounding];
        }
    }

    private updateMaxTreeLevelFromStrategy(strategy: Raven.Client.Indexing.SpatialSearchStrategy) {
        if (strategy === spatialOptions.strategyGeo) {
            this.maxTreeLevel(9);
        } else if (strategy === spatialOptions.strategyQuad) {
            this.maxTreeLevel(23);
        }
    }
}

export = spatialOptions; 
