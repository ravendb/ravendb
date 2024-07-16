import accessManager from "common/shell/accessManager";
import databasesManager from "common/shell/databasesManager";
import intermediateMenuItem from "common/shell/menu/intermediateMenuItem";
import leafMenuItem from "common/shell/menu/leafMenuItem";
import {
    StudioSearchMenuItemType,
    StudioSearchItem,
    StudioSearchItemType,
    StudioSearchItemEvent,
} from "../studioSearchTypes";
import { exhaustiveStringTuple } from "components/utils/common";
import { useEffect, useCallback } from "react";
import IconName from "typings/server/icons";
import { OmniSearch } from "common/omniSearch/omniSearch";
import { useAppUrls } from "components/hooks/useAppUrls";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";

interface UseStudioSearchSyncRegisterParams {
    omniSearch: OmniSearch<StudioSearchItem, StudioSearchItemType>;
    menuItems: menuItem[];
    goToUrl: (url: string, newTab: boolean) => void;
    resetDropdown: () => void;
}

export function useStudioSearchSyncRegister(props: UseStudioSearchSyncRegisterParams) {
    const { omniSearch, menuItems, goToUrl, resetDropdown } = props;

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const collectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const { appUrl } = useAppUrls();

    const goToMenuItem = useCallback(
        (item: leafMenuItem, event: StudioSearchItemEvent) => {
            const url = item.dynamicHash();
            goToUrl(url, event.ctrlKey);
        },
        [goToUrl]
    );

    const goToCollection = useCallback(
        (collectionName: string, event: StudioSearchItemEvent) => {
            const url = appUrl.forDocuments(collectionName, activeDatabaseName);
            goToUrl(url, event.ctrlKey);
        },
        [activeDatabaseName, appUrl, goToUrl]
    );

    const handleDatabaseSwitch = useCallback(
        (databaseName: string, e: StudioSearchItemEvent) => {
            resetDropdown();

            if (e.ctrlKey) {
                window.open(appUrl.forDocumentsByDatabaseName(null, databaseName));
            }
            const db = databasesManager.default.getDatabaseByName(databaseName);
            databasesManager.default.activate(db);
        },
        [resetDropdown, appUrl]
    );

    // Register collections
    useEffect(() => {
        omniSearch.register(
            "collection",
            collectionNames.map((name) => ({
                type: "collection",
                icon: "documents",
                onSelected: (e) => goToCollection(name, e),
                text: name,
            }))
        );
    }, [collectionNames, goToCollection, omniSearch]);

    // Register databases
    useEffect(() => {
        omniSearch.register(
            "database",
            allDatabaseNames.map((databaseName) => ({
                type: "database",
                icon: "database",
                onSelected: (e) => handleDatabaseSwitch(databaseName, e),
                text: databaseName,
            }))
        );
    }, [allDatabaseNames, appUrl, omniSearch, handleDatabaseSwitch]);

    const getMenuItemType = useCallback((route: string): StudioSearchMenuItemType => {
        if (route === "virtual") {
            return null;
        }

        const isDatabaseRoute = route.startsWith(databaseRoutePrefix);

        if (isDatabaseRoute) {
            const databaseRoute = route.replace(databaseRoutePrefix, "");

            for (const [prefix, databaseMenuItemType] of Object.entries(databaseRouteMappings)) {
                if (databaseRoute.startsWith(prefix)) {
                    return databaseMenuItemType;
                }
            }
        }

        return "serverMenuItem";
    }, []);

    // Register menu items
    useEffect(() => {
        const searchItems: StudioSearchItem[] = [];
        const menuLeafs: leafMenuItem[] = [];

        const crawlMenu = (item: menuItem) => {
            if (item instanceof leafMenuItem) {
                menuLeafs.push(item);
            } else if (item instanceof intermediateMenuItem) {
                item.children.forEach(crawlMenu);
            }
        };

        menuItems.forEach(crawlMenu);

        menuLeafs
            .filter((item) => ko.unwrap(item.nav) && !item.alias)
            .forEach((item) => {
                const canHandle = item.requiredAccess
                    ? accessManager.canHandleOperation(item.requiredAccess, activeDatabaseName)
                    : true;

                if (!canHandle) {
                    return;
                }

                const route = Array.isArray(item.route) ? item.route.find((x) => x) : item.route;
                const type = getMenuItemType(route);

                if (type === null) {
                    return;
                }

                const innerActions = (item.search?.innerActions ?? []).map((x) => ({
                    text: x.name,
                    alternativeTexts: x.alternativeNames,
                }));

                searchItems.push({
                    type,
                    text: item.title,
                    route,
                    alternativeTexts: item.search?.alternativeTitles ?? [],
                    icon: item.css.replace("icon-", "") as IconName,
                    onSelected: (e) => goToMenuItem(item, e),
                    innerActions,
                });
            });

        allMenuItemTypes.forEach((type) => {
            omniSearch.register(
                type,
                searchItems.filter((x) => x.type === type)
            );
        });
    }, [activeDatabaseName, menuItems, omniSearch, goToMenuItem, getMenuItemType]);
}

const databaseRoutePrefix = "databases/";

const databaseRouteMappings: Record<string, StudioSearchMenuItemType> = {
    tasks: "tasksMenuItem",
    indexes: "indexesMenuItem",
    query: "indexesMenuItem",
    documents: "documentsMenuItem",
    patch: "documentsMenuItem",
    identities: "documentsMenuItem",
    cmpXchg: "documentsMenuItem",
    settings: "settingsMenuItem",
    manageDatabaseGroup: "settingsMenuItem",
    advanced: "settingsMenuItem",
    status: "statsMenuItem",
};

const allMenuItemTypes = exhaustiveStringTuple<StudioSearchMenuItemType>()(
    "serverMenuItem",
    "documentsMenuItem",
    "indexesMenuItem",
    "tasksMenuItem",
    "settingsMenuItem",
    "statsMenuItem"
);
