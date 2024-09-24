import genUtils from "common/generalUtils";
import generateMenuItems from "common/shell/menu/generateMenuItems";
import intermediateMenuItem from "common/shell/menu/intermediateMenuItem";
import leafMenuItem from "common/shell/menu/leafMenuItem";
import { docsHashes } from "components/utils/docsHashes";

const skippedRoutes: string[] = [
    "databases/tasks/import*details",
    "databases/status/debug*details",
    "admin/settings/debug/advanced*details",
    "whatsNew",
    "virtual",
];

describe("docsHashes", () => {
    it("should contain all routes hashes", () => {
        let allRoutes: string[] = [];

        const menuItems = generateMenuItems({
            db: "someDb",
            isNewVersionAvailable: true,
            isWhatsNewVisible: true,
        });

        const menuLeafs: leafMenuItem[] = [];

        const crawlMenu = (item: menuItem) => {
            if (item instanceof leafMenuItem) {
                menuLeafs.push(item);
            } else if (item instanceof intermediateMenuItem) {
                item.children.forEach(crawlMenu);
            }
        };

        menuItems.forEach(crawlMenu);

        allRoutes = Array.from(new Set(menuLeafs.map((x) => genUtils.getSingleRoute(x.route))))
            .filter((x) => !skippedRoutes.includes(x))
            .sort();

        const definedRoutes = Object.keys(docsHashes).sort();

        expect(definedRoutes).toEqual(allRoutes);
        expect(definedRoutes.every((x: keyof typeof docsHashes) => docsHashes[x])).toBeTruthy();
    });
});
