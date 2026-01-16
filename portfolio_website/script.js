// Shooting Star Cursor (Desktop Only)
if (window.innerWidth > 640) {
    let mouseX = 0;
    let mouseY = 0;
    let cursorX = 0;
    let cursorY = 0;
    const trail = [];
    const trailLength = 15;

    // Create trail container
    const trailContainer = document.createElement('div');
    trailContainer.style.cssText = 'position: fixed; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none; z-index: 9999;';
    document.body.appendChild(trailContainer);

    document.addEventListener('mousemove', (e) => {
        mouseX = e.clientX;
        mouseY = e.clientY;

        // Show cursor on first move
        if (document.body.style.getPropertyValue('--cursor-opacity') === '0') {
            document.body.style.setProperty('--cursor-opacity', '1');
        }

        // Add trail point
        trail.push({ x: mouseX, y: mouseY, time: Date.now() });

        // Limit trail length
        if (trail.length > trailLength) {
            trail.shift();
        }
    });

    function animateCursor() {
        // Smooth cursor following
        cursorX += (mouseX - cursorX) * 0.2;
        cursorY += (mouseY - cursorY) * 0.2;

        // Update CSS variables for cursor position
        document.body.style.setProperty('--cursor-x', `${cursorX}px`);
        document.body.style.setProperty('--cursor-y', `${cursorY}px`);

        // Clear old trail elements
        trailContainer.innerHTML = '';

        // Draw trail
        const now = Date.now();
        trail.forEach((point, index) => {
            const age = now - point.time;
            if (age < 500) { // Trail lasts 500ms
                const star = document.createElement('div');
                const opacity = 1 - (age / 500);
                const size = 8 - (index / trail.length) * 6;
                const blur = 2 + (index / trail.length) * 3;

                star.style.cssText = `
                    position: absolute;
                    left: ${point.x}px;
                    top: ${point.y}px;
                    width: ${size}px;
                    height: ${size}px;
                    background: radial-gradient(circle,
                        rgba(255, 215, 0, ${opacity}) 0%,
                        rgba(0, 212, 255, ${opacity * 0.7}) 50%,
                        rgba(184, 79, 255, ${opacity * 0.5}) 100%);
                    border-radius: 50%;
                    transform: translate(-50%, -50%);
                    filter: blur(${blur}px);
                    pointer-events: none;
                    box-shadow: 0 0 ${10 * opacity}px rgba(255, 215, 0, ${opacity});
                `;

                trailContainer.appendChild(star);
            }
        });

        requestAnimationFrame(animateCursor);
    }

    // Start cursor animation
    requestAnimationFrame(animateCursor);

    // Hide cursor when mouse leaves window
    document.addEventListener('mouseleave', () => {
        document.body.style.setProperty('--cursor-opacity', '0');
    });

    // Scale up cursor on interactive elements
    const addCursorEffects = () => {
        const clickables = document.querySelectorAll('a, button, .btn, .social-link, .project-link, .filter-btn, input, textarea');
        clickables.forEach(el => {
            el.addEventListener('mouseenter', () => {
                document.body.style.setProperty('--cursor-scale', '1.5');
            });
            el.addEventListener('mouseleave', () => {
                document.body.style.setProperty('--cursor-scale', '1');
            });
        });
    };

    // Add effects on load and after dynamic content
    addCursorEffects();
    setTimeout(addCursorEffects, 1000);

    // Create sparkles on click
    document.addEventListener('click', (e) => {
        for (let i = 0; i < 12; i++) {
            const sparkle = document.createElement('div');
            const angle = (Math.PI * 2 * i) / 12;
            const velocity = 2 + Math.random() * 3;
            const size = 4 + Math.random() * 6;

            sparkle.style.cssText = `
                position: fixed;
                left: ${e.clientX}px;
                top: ${e.clientY}px;
                width: ${size}px;
                height: ${size}px;
                background: linear-gradient(135deg, #ffd700, #00d4ff, #b84fff);
                border-radius: 50%;
                pointer-events: none;
                z-index: 10002;
                box-shadow: 0 0 10px rgba(255, 215, 0, 0.8);
            `;

            document.body.appendChild(sparkle);

            let posX = e.clientX;
            let posY = e.clientY;
            let opacity = 1;
            let vx = Math.cos(angle) * velocity;
            let vy = Math.sin(angle) * velocity;

            const animateSparkle = () => {
                posX += vx;
                posY += vy;
                vy += 0.1; // Gravity
                opacity -= 0.02;

                sparkle.style.left = posX + 'px';
                sparkle.style.top = posY + 'px';
                sparkle.style.opacity = opacity;
                sparkle.style.transform = `scale(${opacity})`;

                if (opacity > 0) {
                    requestAnimationFrame(animateSparkle);
                } else {
                    sparkle.remove();
                }
            };

            animateSparkle();
        }
    });
}

// Theme Toggle
const themeToggle = document.getElementById('themeToggle');
const html = document.documentElement;

// Check for saved theme preference or default to light mode
const currentTheme = localStorage.getItem('theme') || 'light';
html.setAttribute('data-theme', currentTheme);
updateThemeIcon(currentTheme);

themeToggle.addEventListener('click', () => {
    const theme = html.getAttribute('data-theme');
    const newTheme = theme === 'light' ? 'dark' : 'light';

    html.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    updateThemeIcon(newTheme);
});

function updateThemeIcon(theme) {
    const icon = themeToggle.querySelector('i');
    if (theme === 'dark') {
        icon.classList.remove('fa-moon');
        icon.classList.add('fa-sun');
    } else {
        icon.classList.remove('fa-sun');
        icon.classList.add('fa-moon');
    }
}

// Mobile Navigation
const hamburger = document.getElementById('hamburger');
const navMenu = document.querySelector('.nav-menu');

hamburger.addEventListener('click', () => {
    hamburger.classList.toggle('active');
    navMenu.classList.toggle('active');
});

// Close mobile menu when clicking on a link
document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', () => {
        hamburger.classList.remove('active');
        navMenu.classList.remove('active');
    });
});

// Smooth Scrolling
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));
        if (target) {
            const offsetTop = target.offsetTop - 70;
            window.scrollTo({
                top: offsetTop,
                behavior: 'smooth'
            });
        }
    });
});

// Typing Effect for Hero Section
const typedTextSpan = document.querySelector('.typed-text');
const textArray = [
    'Data Engineer',
    'Backend Engineer',
    'Data Scientist',
    'ML Engineer',
    'Problem Solver'
];
const typingDelay = 100;
const erasingDelay = 50;
const newTextDelay = 2000;
let textArrayIndex = 0;
let charIndex = 0;

function type() {
    if (charIndex < textArray[textArrayIndex].length) {
        typedTextSpan.textContent += textArray[textArrayIndex].charAt(charIndex);
        charIndex++;
        setTimeout(type, typingDelay);
    } else {
        setTimeout(erase, newTextDelay);
    }
}

function erase() {
    if (charIndex > 0) {
        typedTextSpan.textContent = textArray[textArrayIndex].substring(0, charIndex - 1);
        charIndex--;
        setTimeout(erase, erasingDelay);
    } else {
        textArrayIndex++;
        if (textArrayIndex >= textArray.length) textArrayIndex = 0;
        setTimeout(type, typingDelay + 1100);
    }
}

// Start typing effect when page loads
document.addEventListener('DOMContentLoaded', () => {
    setTimeout(type, newTextDelay + 250);
});

// Project Filtering
const filterButtons = document.querySelectorAll('.filter-btn');
const projectCards = document.querySelectorAll('.project-card');

filterButtons.forEach(button => {
    button.addEventListener('click', () => {
        // Remove active class from all buttons
        filterButtons.forEach(btn => btn.classList.remove('active'));
        // Add active class to clicked button
        button.classList.add('active');

        const filterValue = button.getAttribute('data-filter');

        projectCards.forEach(card => {
            if (filterValue === 'all' || card.getAttribute('data-category') === filterValue) {
                card.classList.remove('hide');
                card.style.animation = 'fadeInUp 0.5s ease';
            } else {
                card.classList.add('hide');
            }
        });
    });
});

// Navbar Background on Scroll
const navbar = document.querySelector('.navbar');
let lastScroll = 0;

window.addEventListener('scroll', () => {
    const currentScroll = window.pageYOffset;

    if (currentScroll <= 0) {
        navbar.classList.remove('scroll-up');
        return;
    }

    if (currentScroll > lastScroll && !navbar.classList.contains('scroll-down')) {
        // Scrolling down
        navbar.classList.remove('scroll-up');
        navbar.classList.add('scroll-down');
    } else if (currentScroll < lastScroll && navbar.classList.contains('scroll-down')) {
        // Scrolling up
        navbar.classList.remove('scroll-down');
        navbar.classList.add('scroll-up');
    }
    lastScroll = currentScroll;
});

// Intersection Observer for Animations
const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
};

const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            entry.target.classList.add('fade-in');
            observer.unobserve(entry.target);
        }
    });
}, observerOptions);

// Observe elements
document.querySelectorAll('.skill-category, .project-card, .timeline-item, .stat').forEach(el => {
    observer.observe(el);
});

// Form Submission
const contactForm = document.getElementById('contactForm');

contactForm.addEventListener('submit', (e) => {
    e.preventDefault();

    // Get form data
    const formData = new FormData(contactForm);
    const data = Object.fromEntries(formData);

    // Here you would typically send the data to a server
    // For now, we'll just show an alert
    console.log('Form submitted:', data);

    // Show success message
    showNotification('Message sent successfully! I\'ll get back to you soon.', 'success');

    // Reset form
    contactForm.reset();
});

// Notification Function
function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.style.cssText = `
        position: fixed;
        top: 100px;
        right: 20px;
        padding: 1rem 1.5rem;
        background: ${type === 'success' ? '#10b981' : '#3b82f6'};
        color: white;
        border-radius: 0.5rem;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
        z-index: 9999;
        animation: slideIn 0.3s ease;
        max-width: 300px;
    `;
    notification.textContent = message;

    // Add to document
    document.body.appendChild(notification);

    // Remove after 3 seconds
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => {
            document.body.removeChild(notification);
        }, 300);
    }, 3000);
}

// Add notification animations to CSS dynamically
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(400px);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }

    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(400px);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);

// Active Nav Link on Scroll
const sections = document.querySelectorAll('section[id]');
const navLinks = document.querySelectorAll('.nav-link');

function activeNavLink() {
    const scrollY = window.pageYOffset;

    sections.forEach(section => {
        const sectionHeight = section.offsetHeight;
        const sectionTop = section.offsetTop - 100;
        const sectionId = section.getAttribute('id');

        if (scrollY > sectionTop && scrollY <= sectionTop + sectionHeight) {
            navLinks.forEach(link => {
                link.classList.remove('active');
                if (link.getAttribute('href') === `#${sectionId}`) {
                    link.classList.add('active');
                }
            });
        }
    });
}

window.addEventListener('scroll', activeNavLink);

// Scroll to Top Button (Optional Enhancement)
const createScrollToTop = () => {
    const scrollBtn = document.createElement('button');
    scrollBtn.innerHTML = '<i class="fas fa-arrow-up"></i>';
    scrollBtn.className = 'scroll-to-top';
    scrollBtn.style.cssText = `
        position: fixed;
        bottom: 30px;
        right: 30px;
        width: 50px;
        height: 50px;
        border-radius: 50%;
        background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
        color: white;
        border: none;
        cursor: pointer;
        opacity: 0;
        transition: opacity 0.3s ease, transform 0.3s ease;
        z-index: 999;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        font-size: 1.2rem;
    `;

    document.body.appendChild(scrollBtn);

    window.addEventListener('scroll', () => {
        if (window.pageYOffset > 300) {
            scrollBtn.style.opacity = '1';
        } else {
            scrollBtn.style.opacity = '0';
        }
    });

    scrollBtn.addEventListener('click', () => {
        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        });
    });

    scrollBtn.addEventListener('mouseenter', () => {
        scrollBtn.style.transform = 'translateY(-5px)';
    });

    scrollBtn.addEventListener('mouseleave', () => {
        scrollBtn.style.transform = 'translateY(0)';
    });
};

createScrollToTop();

// Parallax Effect for Hero Section
window.addEventListener('scroll', () => {
    const scrolled = window.pageYOffset;
    const heroImage = document.querySelector('.hero-image');

    if (heroImage) {
        heroImage.style.transform = `translateY(${scrolled * 0.3}px)`;
    }
});

// Counter Animation for Stats
const animateCounters = () => {
    const counters = document.querySelectorAll('.stat h4');
    const speed = 200;

    counters.forEach(counter => {
        const updateCount = () => {
            const target = parseInt(counter.getAttribute('data-target') || counter.innerText);
            const count = parseInt(counter.innerText.replace(/\D/g, ''));
            const increment = target / speed;

            if (count < target) {
                counter.innerText = Math.ceil(count + increment) + '+';
                setTimeout(updateCount, 1);
            } else {
                counter.innerText = target + '+';
            }
        };

        // Store original value
        if (!counter.getAttribute('data-target')) {
            const originalValue = parseInt(counter.innerText.replace(/\D/g, ''));
            counter.setAttribute('data-target', originalValue);
        }

        // Observe when stat comes into view
        const statObserver = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    counter.innerText = '0+';
                    updateCount();
                    statObserver.unobserve(entry.target);
                }
            });
        }, { threshold: 0.5 });

        statObserver.observe(counter.closest('.stat'));
    });
};

animateCounters();

// Add loading animation
window.addEventListener('load', () => {
    document.body.classList.add('loaded');
});

// Cursor trail effect (optional, modern touch)
let cursorTrail = [];
const maxTrailLength = 20;

document.addEventListener('mousemove', (e) => {
    if (window.innerWidth > 768) { // Only on desktop
        cursorTrail.push({ x: e.clientX, y: e.clientY, time: Date.now() });

        if (cursorTrail.length > maxTrailLength) {
            cursorTrail.shift();
        }
    }
});

// Flowing Data Particles Between Sections (Pipeline Effect)
if (window.innerWidth > 640) {
    const particleCanvas = document.createElement('canvas');
    particleCanvas.style.cssText = 'position: fixed; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none; z-index: 1;';
    document.body.appendChild(particleCanvas);

    const ctx = particleCanvas.getContext('2d');
    particleCanvas.width = window.innerWidth;
    particleCanvas.height = window.innerHeight;

    const particles = [];
    const particleCount = 30;

    class DataParticle {
        constructor() {
            this.reset();
        }

        reset() {
            this.x = Math.random() * particleCanvas.width;
            this.y = -10;
            this.size = Math.random() * 3 + 1;
            this.speedY = Math.random() * 1.5 + 0.5;
            this.speedX = (Math.random() - 0.5) * 0.5;
            this.opacity = Math.random() * 0.5 + 0.3;
            this.color = ['#00d4ff', '#b84fff', '#00ff41'][Math.floor(Math.random() * 3)];
        }

        update() {
            this.y += this.speedY;
            this.x += this.speedX;

            // Reset particle when it goes off screen
            if (this.y > particleCanvas.height) {
                this.reset();
            }
        }

        draw() {
            ctx.beginPath();
            ctx.arc(this.x, this.y, this.size, 0, Math.PI * 2);
            ctx.fillStyle = this.color;
            ctx.globalAlpha = this.opacity;
            ctx.fill();

            // Draw trailing line
            ctx.strokeStyle = this.color;
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(this.x, this.y);
            ctx.lineTo(this.x - this.speedX * 5, this.y - this.speedY * 5);
            ctx.stroke();
        }
    }

    // Create particles
    for (let i = 0; i < particleCount; i++) {
        particles.push(new DataParticle());
    }

    function animateParticles() {
        ctx.clearRect(0, 0, particleCanvas.width, particleCanvas.height);
        ctx.globalAlpha = 1;

        particles.forEach(particle => {
            particle.update();
            particle.draw();
        });

        requestAnimationFrame(animateParticles);
    }

    animateParticles();

    // Handle window resize
    window.addEventListener('resize', () => {
        particleCanvas.width = window.innerWidth;
        particleCanvas.height = window.innerHeight;
    });
}

console.log('%c [PIPELINE_STATUS]: RUNNING ', 'background: #0a0e1a; color: #00ff41; padding: 10px; border: 2px solid #00ff41; font-family: monospace; font-weight: bold;');
console.log('%c >> DATA_FLOW: ACTIVE ', 'color: #00d4ff; font-size: 14px; font-family: monospace;');
