import { rtlRender, waitFor } from "test/rtlTestUtils";
import Tooltip from "react-bootstrap/Tooltip";
import OverlayTriggerWithMotion from "./OverlayTriggerWithMotion";

const overrideTooltip = <Tooltip id="override-hint">Override server-wide analyzer</Tooltip>;

const renderTrigger = (props: Partial<Parameters<typeof OverlayTriggerWithMotion>[0]> = {}) =>
    rtlRender(
        <OverlayTriggerWithMotion overlay={overrideTooltip} {...props}>
            <button type="button">Override</button>
        </OverlayTriggerWithMotion>
    );

describe("OverlayTriggerWithMotion", () => {
    it("shows its tooltip with the shared overlay motion", async () => {
        const { screen, user } = renderTrigger();

        await user.hover(screen.getByRole("button", { name: "Override" }));

        expect(await screen.findByRole("tooltip")).toHaveClass("overlay-motion");
    });

    it("places the tooltip with top and left so the motion can scale it in place", async () => {
        const { screen, user } = renderTrigger();

        await user.hover(screen.getByRole("button", { name: "Override" }));
        const tooltip = await screen.findByRole("tooltip");

        await waitFor(() => expect(tooltip.style.left).not.toBe(""));
        expect(tooltip.style.transform).toBe("");
    });

    it("keeps the Popper modifiers it is given", async () => {
        const modifier = jest.fn();
        const { screen, user } = renderTrigger({
            popperConfig: { modifiers: [{ name: "track", enabled: true, phase: "main", fn: modifier }] },
        });

        await user.hover(screen.getByRole("button", { name: "Override" }));
        await screen.findByRole("tooltip");

        await waitFor(() => expect(modifier).toHaveBeenCalled());
    });

    it("adds the motion to an overlay rendered by a function", async () => {
        const { screen, user } = renderTrigger({
            overlay: (props) => (
                <Tooltip id="override-hint" {...props}>
                    Override server-wide analyzer
                </Tooltip>
            ),
        });

        await user.hover(screen.getByRole("button", { name: "Override" }));

        expect(await screen.findByRole("tooltip")).toHaveClass("overlay-motion");
    });
});
