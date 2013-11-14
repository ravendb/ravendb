/// <reference path="../models/dto.ts" />
define(["require", "exports"], function(require, exports) {
    var documentMetadata = (function () {
        function documentMetadata(dto) {
            if (dto) {
                this.ravenEntityName = dto['Raven-Entity-Name'];
                this.ravenClrType = dto['Raven-Clr-Type'];
                this.nonAuthoritativeInfo = dto['Non-Authoritative-Information'];
                this.id = dto['@id'];
                this.tempIndexScore = dto['Temp-Index-Score'];
                this.lastModified = dto['Last-Modified'];
                this.ravenLastModified = dto['Raven-Last-Modified'];
                this.etag = dto['@etag'];

                for (var property in dto) {
                    if (property !== 'Raven-Entity-Name' && property !== 'Raven-Clr-Type' && property !== 'Non-Authoritative-Information' && property !== '@id' && property !== 'Temp-Index-Score' && property !== 'Last-Modified' && property !== 'Raven-Last-Modified' && property !== '@etag') {
                        this.nonStandardProps = this.nonStandardProps || [];
                        this[property] = dto[property];
                        this.nonStandardProps.push(property);
                    }
                }
            }
        }
        documentMetadata.prototype.toDto = function () {
            var _this = this;
            var dto = {
                'Raven-Entity-Name': this.ravenEntityName,
                'Raven-Clr-Type': this.ravenClrType,
                'Non-Authoritative-Information': this.nonAuthoritativeInfo,
                '@id': this.id,
                'Temp-Index-Score': this.tempIndexScore,
                'Last-Modified': this.lastModified,
                'Raven-Last-Modified': this.ravenLastModified,
                '@etag': this.etag
            };

            if (this.nonStandardProps) {
                this.nonStandardProps.forEach(function (p) {
                    return dto[p] = _this[p];
                });
            }

            return dto;
        };
        return documentMetadata;
    })();

    
    return documentMetadata;
});
//# sourceMappingURL=documentMetadata.js.map
