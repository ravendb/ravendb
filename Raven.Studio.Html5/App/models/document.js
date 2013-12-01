define(["require", "exports", "models/documentMetadata"], function(require, exports, __documentMetadata__) {
    var documentMetadata = __documentMetadata__;

    var document = (function () {
        function document(dto) {
            this.__metadata = new documentMetadata(dto['@metadata']);
            for (var property in dto) {
                if (property !== '@metadata') {
                    this[property] = dto[property];
                }
            }
        }
        document.prototype.getId = function () {
            return this.__metadata.id;
        };

        document.prototype.getDocumentPropertyNames = function () {
            var propertyNames = [];
            for (var property in this) {
                var isMeta = property === '__metadata' || property === '__moduleId__';
                var isFunction = typeof this[property] === 'function';
                if (!isMeta && !isFunction) {
                    propertyNames.push(property);
                }
            }

            return propertyNames;
        };

        document.prototype.toDto = function (includeMeta) {
            if (typeof includeMeta === "undefined") { includeMeta = false; }
            var dto = { '@metadata': undefined };
            var properties = this.getDocumentPropertyNames();
            for (var i = 0; i < properties.length; i++) {
                var property = properties[i];
                dto[property] = this[property];
            }

            if (includeMeta && this.__metadata) {
                dto['@metadata'] = this.__metadata.toDto();
            }

            return dto;
        };

        document.empty = function () {
            var emptyDto = {
                '@metadata': {},
                'Name': '...'
            };
            return new document(emptyDto);
        };
        return document;
    })();

    
    return document;
});
//# sourceMappingURL=document.js.map
