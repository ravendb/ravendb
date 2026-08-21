import { Pencil } from "lucide-react";
import type { Control } from "react-hook-form";
import { InfoHint } from "@/components/data/info-hint";
import { FormSwitch } from "@/components/form/form-switch";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import type { LogConfigurationFormValues } from "./log-configuration-form-values";
import { LogDestinations } from "./log-destinations";
import type { LogDestination } from "./log-settings-summary";

type LogSettingsToolbarProps = {
    control: Control<LogConfigurationFormValues>;
    canPersist: boolean;
    destinations: LogDestination[];
    dirtyCount: number;
    isEditing: boolean;
    isSaving: boolean;
    onDiscard: () => void;
    onEdit: () => void;
};

/**
 * Pinned to the top of the scrolling page, matching the widget theme editor. The save row used to sit
 * below both cards, which put it 117px under the fold of a 1366x768 laptop - so committing a change
 * made at the top of the form meant scrolling to the bottom to find the button.
 *
 * Where output goes and what you can do about it share one line: they are read together, and a
 * separate status line above them only ever restated what the buttons already say.
 *
 * The page reads before it writes: Edit is the only action until the operator asks for the controls,
 * and it is then replaced by the pair that ends editing.
 */
export function LogSettingsToolbar({
    control,
    canPersist,
    destinations,
    dirtyCount,
    isEditing,
    isSaving,
    onDiscard,
    onEdit,
}: LogSettingsToolbarProps) {
    return (
        <div className="sticky top-0 z-10 -mx-2 mb-2 flex flex-wrap items-center justify-between gap-x-4 gap-y-3 bg-surface1 px-2 py-2 dark:bg-surface2">
            <LogDestinations destinations={destinations} />

            {/* One group, so the toolbar stays a two-part bar however many actions are in it. */}
            <div className="flex flex-wrap items-center gap-3">
                {isEditing ? (
                    <>
                        {/* Describes what happens when you commit, not what the appliance logs, so it
                            belongs with the save controls rather than at the end of the form. */}
                        <div className="flex items-center gap-1.5">
                            <FormSwitch
                                control={control}
                                name="shouldPersist"
                                label="Keep after restart"
                                disabled={!canPersist}
                            />
                            <InfoHint
                                content={
                                    canPersist
                                        ? "Also writes these settings to quill.nlog.config so they survive a restart. With this off, the change applies to the running appliance only."
                                        : "No writable quill.nlog.config is configured, so changes apply to the running appliance only and are lost on restart."
                                }
                            />
                        </div>
                        <Button type="button" variant="outline" size="sm" onClick={onDiscard} disabled={isSaving}>
                            Discard
                        </Button>
                        {/* Counting the changes in the label keeps "what am I committing" answerable
                            without spending a line on it. */}
                        <Button type="submit" size="sm" disabled={dirtyCount === 0 || isSaving}>
                            {isSaving && <Spinner />}
                            {dirtyCount === 0
                                ? "Save changes"
                                : `Save ${dirtyCount} change${dirtyCount === 1 ? "" : "s"}`}
                        </Button>
                    </>
                ) : (
                    <Button type="button" variant="outline" size="sm" onClick={onEdit}>
                        <Pencil aria-hidden="true" />
                        Edit
                    </Button>
                )}
            </div>
        </div>
    );
}
