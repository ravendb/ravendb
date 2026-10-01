import { useState } from "react";
import { TaskCardCategory, TaskCardInfo } from "components/pages/database/tasks/shared/AddTaskCardList";

export function useTaskCardFilters(categories: TaskCardCategory[]) {
    const [searchText, setSearchText] = useState("");
    const [selectedCategories, setSelectedCategories] = useState<string[]>([]);

    const toggleCategory = (categoryName: string) => {
        setSelectedCategories((prev) =>
            prev.includes(categoryName) ? prev.filter((c) => c !== categoryName) : [...prev, categoryName]
        );
    };

    const resetCategories = () => setSelectedCategories([]);

    const searchFilteredTasks = categories
        .map((category) => ({
            ...category,
            tasks: category.tasks.filter((task) => matchesSearchText(task, searchText)),
        }))
        .filter((category) => category.tasks.length > 0);

    const filteredTasks = searchFilteredTasks.filter(
        (category) => selectedCategories.length === 0 || selectedCategories.includes(category.categoryName)
    );

    const allCategories = categories.map((c) => ({
        categoryName: c.categoryName,
        categoryIcon: c.categoryIcon,
    }));

    return {
        filteredTasks,
        searchFilteredTasks,
        allCategories,
        searchText,
        setSearchText,
        selectedCategories,
        toggleCategory,
        resetCategories,
    };
}

const matchesSearchText = (task: TaskCardInfo, searchText: string) => {
    if (!searchText) {
        return true;
    }

    const searchLower = searchText.trim().toLowerCase();
    return task.title.toLowerCase().includes(searchLower) || task.description.toLowerCase().includes(searchLower);
};
