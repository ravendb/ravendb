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

        _.assign(options, extraOptions);
        return selector.popover(options)
            .on("mouseenter", function () {
                const self = this;
                $(self).popover("show");
                $(".popover")
                    .on("mouseleave", () => $(self).popover("hide"));
            }).on("mouseleave", function () {
                const self = this;
                setTimeout(() => {
                    if (!$(".popover:hover").length) {
                        $(self).popover("hide");
                    }
                }, 300);
            });
    }
}

export = popoverUtils;
