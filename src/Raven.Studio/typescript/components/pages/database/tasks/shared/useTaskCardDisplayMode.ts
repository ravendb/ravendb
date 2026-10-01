import { useEffect, useState } from "react";
import studioSettings = require("common/settings/studioSettings");
import { TaskCardDisplayMode } from "components/pages/database/tasks/shared/AddTaskCardList";

export function useTaskCardDisplayMode() {
    const [displayMode, setDisplayModeState] = useState<TaskCardDisplayMode>("expanded");

    useEffect(() => {
        let disposed = false;

        studioSettings.default.globalSettings().done((settings) => {
            if (!disposed) {
                setDisplayModeState(settings.ongoingTaskDisplayMode.getValue());
            }
        });

        return () => {
            disposed = true;
        };
    }, []);

    const setDisplayMode = (mode: TaskCardDisplayMode) => {
        setDisplayModeState(mode);
        studioSettings.default.globalSettings().done((settings) => settings.ongoingTaskDisplayMode.setValue(mode));
    };

    return { displayMode, setDisplayMode };
}
