import { useState } from "react";
import Button from "react-bootstrap/Button";
import { rtlRender } from "test/rtlTestUtils";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import { useDirtyFlag } from "./useDirtyFlag";

function Form({ isDirty }: { isDirty: boolean }) {
    useDirtyFlag(isDirty);
    return <span>{isDirty ? "Dirty form" : "Clean form"}</span>;
}

function Page() {
    const [isDirtyFormShown, setIsDirtyFormShown] = useState(true);

    return (
        <>
            {isDirtyFormShown && <Form isDirty />}
            <Form isDirty={false} />
            <Button onClick={() => setIsDirtyFormShown(false)}>Hide dirty form</Button>
        </>
    );
}

describe("useDirtyFlag", () => {
    it("keeps the page dirty while any form is dirty", () => {
        rtlRender(<Page />);

        expect(mockHooks.useDirtyFlag.isDirty).toBe(true);
    });

    it("clears the page once the dirty form unmounts", async () => {
        const { screen, fireClick } = rtlRender(<Page />);

        await fireClick(screen.getByText("Hide dirty form"));

        expect(mockHooks.useDirtyFlag.isDirty).toBe(false);
    });
});
