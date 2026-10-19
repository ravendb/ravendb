import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import databaseSettings from "common/settings/databaseSettings";
import studioSettings from "common/settings/studioSettings";
import database from "models/resources/database";

function createTracker() {
    const settingsTasks: JQueryDeferred<databaseSettings>[] = [];
    jest.spyOn(studioSettings.default, "forDatabase").mockImplementation(() => {
        const task = $.Deferred<databaseSettings>();
        settingsTasks.push(task);
        return task;
    });

    return { tracker: new activeDatabaseTracker(), settingsTasks };
}

const createDatabase = (name: string) => ({ name }) as database;
const createSettings = () => ({}) as databaseSettings;

describe("activeDatabaseTracker", () => {
    afterEach(() => {
        jest.restoreAllMocks();
    });

    it("activates only the database selected last when the selection changes during an activation", () => {
        const { tracker, settingsTasks } = createTracker();
        const db2 = createDatabase("db2");

        const db1Activation = tracker.onActivation(createDatabase("db1"));
        const db2Activation = tracker.onActivation(db2);

        expect(db1Activation.state()).toBe("rejected");
        expect(tracker.getActivationTask(createDatabase("db1"))).toBeNull();
        expect(tracker.getActivationTask(createDatabase("db2"))).toBe(db2Activation);

        settingsTasks[0].resolve(createSettings());

        expect(tracker.database()).toBeUndefined();

        settingsTasks[1].resolve(createSettings());

        expect(db2Activation.state()).toBe("resolved");
        expect(tracker.database()).toBe(db2);
        expect(tracker.getActivationTask(createDatabase("db2")).state()).toBe("resolved");
    });

    it("rejects the activation when the database settings fail to load", () => {
        const { tracker, settingsTasks } = createTracker();

        const activation = tracker.onActivation(createDatabase("db1"));
        settingsTasks[0].reject();

        expect(activation.state()).toBe("rejected");
        expect(tracker.database()).toBeUndefined();
        expect(tracker.getActivationTask(createDatabase("db1"))).toBeNull();
    });
});
