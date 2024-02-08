import endpoints from "endpoints";

class twoFactorHelper {
    static handledTwoFactorNeed = false;

    static twoFactorNeeded() {
        if (!twoFactorHelper.handledTwoFactorNeed) {
            window.location.assign(location.origin + endpoints.global.studio._2faIndex_html);
            
            twoFactorHelper.handledTwoFactorNeed = true;
        }
    }
}

export = twoFactorHelper;
