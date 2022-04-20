import { buildQueries, queryHelpers } from "@testing-library/react";

const queryAllByName = (...args: any[]) => (queryHelpers as any).queryAllByAttribute("name", ...args);

const getMultipleError = (c: any, name: any) => `Found multiple elements with the name attribute of: ${name}`;
const getMissingError = (c: any, name: any) => `Unable to find an element with the name attribute of: ${name}`;

const [queryByName, getAllByName, getByName, findAllByName, findByName] = buildQueries(
    queryAllByName,
    getMultipleError,
    getMissingError
);

export { queryByName, queryAllByName, getByName, getAllByName, findAllByName, findByName };
