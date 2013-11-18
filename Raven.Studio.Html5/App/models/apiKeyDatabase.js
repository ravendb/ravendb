define(["require", "exports"], function(require, exports) {
    var apiKeyDatabase = (function () {
        function apiKeyDatabase(dto) {
            this.name = ko.observable();
            this.admin = ko.observable();
            this.readOnly = ko.observable();
            this.name(dto.name);
            this.admin(dto.admin);
            this.readOnly(dto.readOnly);
        }
        return apiKeyDatabase;
    })();

    
    return apiKeyDatabase;
});
//# sourceMappingURL=apiKeyDatabase.js.map
