define(["require", "exports"], function(require, exports) {
    var alertType;
    (function (alertType) {
        alertType[alertType["info"] = 0] = "info";
        alertType[alertType["success"] = 1] = "success";
        alertType[alertType["warning"] = 2] = "warning";
        alertType[alertType["danger"] = 3] = "danger";
    })(alertType || (alertType = {}));

    
    return alertType;
});
//# sourceMappingURL=alertType.js.map
