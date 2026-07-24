const navToggle = document.querySelector(".nav-toggle");
const siteNav = document.querySelector(".site-nav");

if (navToggle && siteNav) {
  navToggle.addEventListener("click", () => {
    const isOpen = navToggle.getAttribute("aria-expanded") === "true";
    navToggle.setAttribute("aria-expanded", String(!isOpen));
    siteNav.classList.toggle("is-open", !isOpen);
  });

  siteNav.querySelectorAll("a").forEach((link) => {
    link.addEventListener("click", () => {
      navToggle.setAttribute("aria-expanded", "false");
      siteNav.classList.remove("is-open");
    });
  });
}

const publicationSearch = document.querySelector("#publication-search");
const publicationFilter = document.querySelector("#publication-filter");
const publicationCards = [...document.querySelectorAll(".publication-card")];
const publicationEmpty = document.querySelector("#publication-empty");
const publicationCount = document.querySelector("#publication-count");

function filterPublications() {
  if (!publicationCards.length) return;

  const search = publicationSearch?.value.trim().toLowerCase() || "";
  const filter = publicationFilter?.value || "all";
  let visible = 0;

  publicationCards.forEach((card) => {
    const text = card.textContent.toLowerCase();
    const category = card.dataset.category;
    const matchesSearch = text.includes(search);
    const matchesFilter = filter === "all" || category === filter;
    const shouldShow = matchesSearch && matchesFilter;
    card.hidden = !shouldShow;
    if (shouldShow) visible += 1;
  });

  if (publicationEmpty) publicationEmpty.hidden = visible !== 0;
  if (publicationCount) {
    publicationCount.textContent = `${visible} paper${visible === 1 ? "" : "s"}`;
  }
}

publicationSearch?.addEventListener("input", filterPublications);
publicationFilter?.addEventListener("change", filterPublications);
filterPublications();
