import Certificates from "./Certificates";
import { rtlRender } from "test/rtlTestUtils";

describe("Certificates", () => {
    // TODO: Add more tests
    it("should render", () => {
        const { screen } = rtlRender(<Certificates />);
        expect(screen.getByText("Certificates")).toBeInTheDocument();
    });
});
