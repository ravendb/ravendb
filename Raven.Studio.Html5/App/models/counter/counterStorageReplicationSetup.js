define(["require", "exports", "models/counter/counterStorageReplicationDestination"], function(require, exports, counterStorageReplicationDestination) {
    var counterStorageReplicationSetup = (function () {
        function counterStorageReplicationSetup(dto) {
            this.destinations = ko.observableArray().extend({ required: true });
            this.destinations(dto.Destinations.map(function (dest) {
                return new counterStorageReplicationDestination(dest);
            }));
        }
        counterStorageReplicationSetup.prototype.toDto = function () {
            return {
                Destinations: this.destinations().map(function (dest) {
                    return dest.toDto();
                })
            };
        };
        return counterStorageReplicationSetup;
    })();

    
    return counterStorageReplicationSetup;
});
//# sourceMappingURL=counterStorageReplicationSetup.js.map
