const categoryForm = document.getElementById("categoryForm");
const productForm = document.getElementById("productForm");
const categoryName = document.getElementById("categoryName");
const categorySelect = document.getElementById("categorySelect");
const productId = document.getElementById("productId");
const productName = document.getElementById("productName");
const productPrice = document.getElementById("productPrice");
const productUnit = document.getElementById("productUnit");
const productAliases = document.getElementById("productAliases");
const productRows = document.getElementById("productRows");
const statusText = document.getElementById("statusText");
const saveProductBtn = document.getElementById("saveProductBtn");
const cancelEditBtn = document.getElementById("cancelEditBtn");

let categories = [];
let products = [];

function setStatus(text, isError = false) {
    statusText.textContent = text;
    statusText.style.color = isError ? "#b00020" : "#374151";
}

function resetProductForm() {
    productId.value = "";
    productForm.reset();
    saveProductBtn.textContent = "Save Product";
}

function renderCategoryOptions() {
    categorySelect.innerHTML = "";
    categories.forEach((c) => {
        const opt = document.createElement("option");
        opt.value = String(c.id);
        opt.textContent = c.name;
        categorySelect.appendChild(opt);
    });
}

function renderProducts() {
    const categoryById = Object.fromEntries(categories.map((c) => [c.id, c.name]));
    const rows = products
        .sort((a, b) => Number(b.is_active) - Number(a.is_active) || a.name.localeCompare(b.name))
        .map((p) => {
            const status = Number(p.is_active) === 1 ? "Active" : "Inactive";
            const action = Number(p.is_active) === 1
                ? `<button class="btn" onclick="deactivateProduct(${p.id})">Deactivate</button>`
                : "-";
            return `
                <tr>
                    <td>${p.id}</td>
                    <td>${p.name}</td>
                    <td>${categoryById[p.category_id] || "-"}</td>
                    <td>${Number(p.price_per_unit).toFixed(2)}</td>
                    <td>${p.base_unit}</td>
                    <td>${p.aliases || "-"}</td>
                    <td>${status}</td>
                    <td>
                        <button class="btn" onclick="startEdit(${p.id})">Edit</button>
                        ${action}
                    </td>
                </tr>
            `;
        })
        .join("");
    productRows.innerHTML = rows || `<tr><td colspan="8">No products yet.</td></tr>`;
}

async function fetchCatalog() {
    try {
        const res = await fetch("/admin/api/catalog");
        if (!res.ok) {
            setStatus("Could not load catalog. Please login again.", true);
            return;
        }
        const data = await res.json();
        categories = data.categories || [];
        products = data.products || [];
        renderCategoryOptions();
        renderProducts();
        setStatus("Catalog loaded.");
    } catch (err) {
        setStatus("Server error while loading catalog.", true);
    }
}

window.startEdit = function startEdit(id) {
    const p = products.find((item) => item.id === id);
    if (!p) return;
    productId.value = String(p.id);
    productName.value = p.name;
    categorySelect.value = String(p.category_id);
    productPrice.value = String(p.price_per_unit);
    productUnit.value = p.base_unit;
    productAliases.value = p.aliases || "";
    saveProductBtn.textContent = "Update Product";
    setStatus(`Editing product #${id}`);
};

window.deactivateProduct = async function deactivateProduct(id) {
    if (!confirm("Deactivate this product?")) return;
    try {
        const res = await fetch(`/admin/api/products/${id}`, { method: "DELETE" });
        const data = await res.json();
        if (!res.ok) {
            setStatus(data.error || "Could not deactivate product.", true);
            return;
        }
        setStatus(`Product #${id} deactivated.`);
        await fetchCatalog();
    } catch (err) {
        setStatus("Server error while deactivating product.", true);
    }
};

categoryForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const name = categoryName.value.trim();
    if (!name) return;

    try {
        const res = await fetch("/admin/api/categories", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name }),
        });
        const data = await res.json();
        if (!res.ok) {
            setStatus(data.error || "Could not create category.", true);
            return;
        }
        categoryName.value = "";
        setStatus(`Category "${data.name}" created.`);
        await fetchCatalog();
    } catch (err) {
        setStatus("Server error while creating category.", true);
    }
});

productForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const payload = {
        name: productName.value.trim(),
        category_id: Number(categorySelect.value),
        price_per_unit: Number(productPrice.value),
        base_unit: productUnit.value,
        aliases: productAliases.value.trim(),
    };

    if (!payload.name || !payload.category_id || !payload.price_per_unit || !payload.base_unit) {
        setStatus("Please fill all required fields.", true);
        return;
    }

    const editingId = productId.value.trim();
    const url = editingId ? `/admin/api/products/${editingId}` : "/admin/api/products";
    const method = editingId ? "PUT" : "POST";

    try {
        const res = await fetch(url, {
            method,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) {
            setStatus(data.error || "Could not save product.", true);
            return;
        }
        setStatus(editingId ? `Product #${editingId} updated.` : `Product #${data.id} created.`);
        resetProductForm();
        await fetchCatalog();
    } catch (err) {
        setStatus("Server error while saving product.", true);
    }
});

cancelEditBtn.addEventListener("click", () => {
    resetProductForm();
    setStatus("Edit cancelled.");
});

fetchCatalog();
