// Function to handle Source Selection
function selectSource(sourceName) {
    localStorage.setItem('selectedSource', sourceName);
    window.location.href = 'dashboard.html';
}

document.addEventListener('DOMContentLoaded', () => {
    // --- Login Page Logic ---
    const loginForm = document.getElementById('login-form');
    if (loginForm) {
        // Tab Switching Logic
        const tabs = document.querySelectorAll('.tab-btn');
        const signupForm = document.getElementById('signup-form');

        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                tabs.forEach(t => t.classList.remove('active'));
                tab.classList.add('active');

                const target = tab.dataset.tab;
                if (target === 'login') {
                    loginForm.style.display = 'block';
                    loginForm.classList.remove('active');
                    void loginForm.offsetWidth; // Trigger reflow
                    loginForm.classList.add('active');
                    signupForm.style.display = 'none';
                } else {
                    loginForm.style.display = 'none';
                    signupForm.style.display = 'block';
                    signupForm.classList.remove('active');
                    void signupForm.offsetWidth;
                    signupForm.classList.add('active');
                }
            });
        });

        // Handle Login Submission
        const form = loginForm.querySelector('form');
        form.addEventListener('submit', (e) => {
            e.preventDefault();
            // Simulate login delay
            const btn = form.querySelector('button');
            const originalText = btn.innerText;
            btn.innerText = 'Connexion...';

            setTimeout(() => {
                window.location.href = 'sources.html';
            }, 800);
        });
    }

    // --- Dashboard Logic ---
    if (window.location.pathname.includes('dashboard.html')) {
        const savedSource = localStorage.getItem('selectedSource');
        if (savedSource) {
            const badgeText = document.getElementById('source-name');
            if (badgeText) {
                badgeText.innerText = savedSource;
            }
        }

        // Sidebar Navigation Logic
        const navItems = document.querySelectorAll('.nav-item');
        const views = document.querySelectorAll('.content-view');

        navItems.forEach(item => {
            if (item.dataset.target) {
                item.addEventListener('click', (e) => {
                    e.preventDefault();

                    // Remove active from all navs
                    navItems.forEach(nav => nav.classList.remove('active'));
                    item.classList.add('active');

                    // Hide all views
                    views.forEach(view => view.classList.remove('active'));

                    // Show target view
                    const targetId = item.dataset.target;
                    const targetView = document.getElementById(targetId);
                    if (targetView) {
                        targetView.classList.add('active');
                    }
                });
            }
        });
    }
});
