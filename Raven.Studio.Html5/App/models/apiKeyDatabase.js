define(["require", "exports"], function(require, exports) {
    var apiKeyDatabase = (function () {
        function apiKeyDatabase(dto) {
            this.tenantId = ko.observable();
            this.admin = ko.observable();
            this.readOnly = ko.observable();
            this.tenantId(dto.TenantId);
            this.admin(dto.Admin);
            this.readOnly(dto.ReadOnly);
        }
        return apiKeyDatabase;
    })();

    
    return apiKeyDatabase;
});
//# sourceMappingURL=apiKeyDatabase.js.map
