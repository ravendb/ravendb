import { buildQueries, queryHelpers } from "@testing-library/react";

const queryAllByClassName = (element: HTMLElement, className: string) => {
    return queryHelpers.queryAllByAttribute("class", element, (content: string, element: HTMLElement) => element.classList.contains(className));
};

const getMultipleError = (c: any, name: any) =>
    `Found multiple elements with class name of: ${name}`;
const getMissingError = (c: any, name: any) =>
    `Unable to find an element with the class name of: ${name}`;

const [
    queryByClassName,
    getAllByClassName,
    getByClassName,
    findAllByClassName,
    findByClassName,
] = buildQueries(queryAllByClassName, getMultipleError, getMissingError);

export {
    queryByClassName,
    queryAllByClassName,
    getByClassName,
    getAllByClassName,
    findAllByClassName,
    findByClassName
};
