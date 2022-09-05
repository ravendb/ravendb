/// <reference path="../../typings/tsd.d.ts"/>

/**
 * This is helper class to build boostrap multiselect with support for custom awesome checkboxes
 *
 * https://github.com/davidstutz/bootstrap-multiselect/issues/576
 */
class awesomeMultiselect {

    // used for generating unique label/ids
    static instanceNumber = 1;

    /**
     * Build dropdown
     * @param object
     */
    static build(object: JQuery, customizeOptions?: (opts: any) => void): void {
        const opts = {
            templates: {
                li: '<li><div class="checkbox"><label></label></div></li>'
            }
        };

        if (customizeOptions) {
            customizeOptions(opts);
        }

        object.multiselect(opts);

        if (object.data('instanceData')) {
            throw new Error("Object was already initialized. Use rebuild instead.");
        }
        object.data('instanceData', awesomeMultiselect.instanceNumber);
        awesomeMultiselect.instanceNumber++;

        awesomeMultiselect.fixAwesomeCheckboxes(object);
    }


    /**
     * Update multiselect
     * @param object
     */
    static rebuild(object: JQuery): void {
        if (!object.data('instanceData')) {
            throw new Error("Please initialize multiselect using awesomeMultiselect.build before calling rebuild");
        }
        object.multiselect('rebuild');
        awesomeMultiselect.fixAwesomeCheckboxes(object);
    }

    private static fixAwesomeCheckboxes(object: JQuery) {
        const instanceId = <number> object.data('instanceData');
        $('.multiselect-container .checkbox', object.parent()).each(function (index) {
            const $self = $(this);
            const id = 'multiselect-' + instanceId + "-" + index;
            const $input = $self.find('input');
            const $label = $self.find('label');

            // check if DOM was already modified
            if (!$label.attr('for')) {
                $input.detach();
                $input.prependTo($self);

                $self.click(e => e.stopPropagation());
            }

            $label.attr('for', id);
            $input.attr('id', id);
           
        });
    }
}

export = awesomeMultiselect;
