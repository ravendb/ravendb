/// <reference path="../../typings/tsd.d.ts"/>

class popoverUtils {
    static longPopoverTemplate = `<div class="popover popover-lg" role="tooltip"><div class="arrow"></div><h3 class="popover-title"></h3><div class="popover-content"></div></div>`;

    static longWithHover(selector: JQuery, extraOptions: PopoverOptions): JQuery {
        const options = {
            html: true,
            trigger: "manual",
            template: popoverUtils.longPopoverTemplate,
            container: "body",
            animation: true
        };
        
        let overElement = false;
        let hideHandler: number = undefined;
        
        let lastHideTime = null as number;

        _.assign(options, extraOptions);
        const popover = selector.popover(options);
        
        const scheduleHide = (self: HTMLElement, tip: JQuery) => hideHandler = setTimeout(() => {
            const elementStillInDom = document.contains(self);
            if (elementStillInDom) {
                if (!overElement) {
                    $(self).popover("hide");
                    lastHideTime = new Date().getTime();
                }
            } else {
                $(tip).remove();
            }
           
        }, 300);
        
        const maybeCancelHide = () => {
            if (hideHandler) {
                clearTimeout(hideHandler);
            }
            hideHandler = undefined;
        };
        
        popover.one("shown.bs.popover", () => {
            const $tip = popover.data('bs.popover').$tip;
            
            $tip
                .on("mouseleave.popover", () => {
                    overElement = false;
                    scheduleHide(popover[0], $tip);
                })
                .on("mouseenter.popover", () => {
                    overElement = true;
                    maybeCancelHide();
                })
                .data("popover-utils-init", true);
        });
        
        return popover
            .on("mouseenter", function () {
                overElement = true;
                const self = this;
                
                const sinceLastHide = new Date().getTime() - lastHideTime;
                if (sinceLastHide <= 150) {
                    // since bootstrap emulates hide event 150 milis after hide
                    // we schedule next show right after element will be actually removed from DOM
                    setTimeout(() => {
                        $(self).popover("show");
                    }, 155 - sinceLastHide);
                } else {
                    $(self).popover("show");
                    maybeCancelHide();
                }
            }).on("mouseleave", function () {
                const self = this;
                const $tip = $(self).data('bs.popover').$tip;
                overElement = false;
                scheduleHide(self, $tip);
            });
    }
}

export = popoverUtils;
