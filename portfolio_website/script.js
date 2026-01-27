// =======================
// LIVING IDENTITY SYSTEM
// =======================

// Morph identity text every 3 seconds
const identityElement = document.querySelector('.identity-morph');
let identities = [];
let currentIdentityIndex = 0;

if (identityElement) {
    identities = JSON.parse(identityElement.dataset.identities);

    function morphIdentity() {
        currentIdentityIndex = (currentIdentityIndex + 1) % identities.length;
        identityElement.textContent = identities[currentIdentityIndex];
    }

    setInterval(morphIdentity, 3000);
    identityElement.textContent = identities[0];
}

// =======================
// CURSOR-BASED COLOR SHIFT
// =======================

let mouseX = 0;
let mouseY = 0;

document.addEventListener('mousemove', (e) => {
    mouseX = e.clientX;
    mouseY = e.clientY;

    // Update cursor glow position
    const cursorGlow = document.querySelector('.cursor-glow');
    if (cursorGlow) {
        cursorGlow.style.left = mouseX + 'px';
        cursorGlow.style.top = mouseY + 'px';
    }

    // Update cursor position for deep sea lighting effect
    document.documentElement.style.setProperty('--cursor-x', mouseX + 'px');
    document.documentElement.style.setProperty('--cursor-y', mouseY + 'px');

    // Calculate color shift based on cursor position
    const windowWidth = window.innerWidth;
    const windowHeight = window.innerHeight;

    const hueShift = (mouseX / windowWidth) * 60; // 0-60 degree shift
    const accentShift = (mouseY / windowHeight) * 60;

    const primaryHue = 200 + hueShift;
    const accentHue = 280 + accentShift;

    document.documentElement.style.setProperty('--primary-hue', primaryHue);
    document.documentElement.style.setProperty('--accent-hue', accentHue);
});

// =======================
// WORLD CANVAS ANIMATION
// =======================

const canvas = document.getElementById('worldCanvas');
const ctx = canvas.getContext('2d');

canvas.width = window.innerWidth;
canvas.height = window.innerHeight;

// Particles for background
const particles = [];
const particleCount = 100;

class Particle {
    constructor() {
        this.x = Math.random() * canvas.width;
        this.y = Math.random() * canvas.height;
        this.size = Math.random() * 2;
        this.speedX = (Math.random() - 0.5) * 0.5;
        this.speedY = (Math.random() - 0.5) * 0.5;
    }

    update() {
        this.x += this.speedX;
        this.y += this.speedY;

        if (this.x < 0 || this.x > canvas.width) this.speedX *= -1;
        if (this.y < 0 || this.y > canvas.height) this.speedY *= -1;
    }

    draw() {
        ctx.fillStyle = 'rgba(0, 212, 255, 0.5)';
        ctx.beginPath();
        ctx.arc(this.x, this.y, this.size, 0, Math.PI * 2);
        ctx.fill();
    }
}

// Initialize particles
for (let i = 0; i < particleCount; i++) {
    particles.push(new Particle());
}

function animateCanvas() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    particles.forEach(particle => {
        particle.update();
        particle.draw();
    });

    // Draw connections
    particles.forEach((p1, i) => {
        particles.slice(i + 1).forEach(p2 => {
            const dx = p1.x - p2.x;
            const dy = p1.y - p2.y;
            const distance = Math.sqrt(dx * dx + dy * dy);

            if (distance < 100) {
                ctx.strokeStyle = `rgba(0, 212, 255, ${0.2 * (1 - distance / 100)})`;
                ctx.lineWidth = 0.5;
                ctx.beginPath();
                ctx.moveTo(p1.x, p1.y);
                ctx.lineTo(p2.x, p2.y);
                ctx.stroke();
            }
        });
    });

    requestAnimationFrame(animateCanvas);
}

animateCanvas();

// Resize canvas
window.addEventListener('resize', () => {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
});

// =======================
// WORLD NAVIGATION
// =======================

const compassButtons = document.querySelectorAll('.compass-btn');
const worldSections = document.querySelectorAll('.world-section');
const identityMorph = document.querySelector('.identity-morph');

compassButtons.forEach(btn => {
    btn.addEventListener('click', () => {
        const targetWorld = btn.dataset.world;

        // Hide home intro
        const homeIntro = document.getElementById('home-intro');
        if (homeIntro) {
            homeIntro.classList.remove('active');
        }

        // Hide identity morph
        document.body.classList.add('world-active');

        // Switch worlds
        worldSections.forEach(section => {
            if (section.id === targetWorld) {
                section.classList.add('active');
            } else {
                section.classList.remove('active');
            }
        });

        // Highlight active compass button
        compassButtons.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
    });
});

// Return to identity on ESC
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        document.body.classList.remove('world-active');
        worldSections.forEach(section => section.classList.remove('active'));
        compassButtons.forEach(b => b.classList.remove('active'));
    }
});

// =======================
// PROJECT CITY INTERACTIONS
// =======================

const projectData = {
    patsnap: {
        title: 'Patsnap Database Redesign',
        role: 'Data Engineer / Backend Engineer',
        tech: ['Python', 'PostgreSQL', 'ETL', 'Database Design'],
        description: 'Redesigned and refactored legacy database architecture for Patsnap Eureka platform.',
        achievements: [
            'Achieved faster searches and 5% lower storage costs',
            'Implemented incremental processing with traceable fingerprints for 100% of the data pipeline',
            'Implemented domain-based rules processing across full dataset for data validation',
            'Web crawled public websites to increase and improve the database'
        ]
    },
    llm: {
        title: 'LLM-Enhanced ML Agent',
        role: 'Data Scientist',
        tech: ['Python', 'TensorFlow', 'LLMs', 'NLP'],
        description: 'Improved in-house machine learning agent by incorporating Large Language Models for financial compliance.',
        achievements: [
            '10% efficiency increase in alert processing',
            '12% reduction in false negative rate',
            'Integrated state-of-the-art LLM capabilities',
            'Fine-tuned models for domain-specific tasks'
        ]
    },
    semiconductor: {
        title: 'Semiconductor Testing Dashboard',
        role: 'DTE Modelling Intern',
        tech: ['Python', 'Microsoft BI', 'ML', 'Data Visualization'],
        description: 'Automated verification of wafer machine results and developed ML models for semiconductor testing.',
        achievements: [
            'Reduced verification time from a week to 2 days',
            'Trained models to identify test types and discrepancy severity',
            'Linked to Microsoft BI dashboard with color-coded severity indicators',
            'Automated bad die identification process'
        ]
    },
    etl: {
        title: 'ETL Pipeline for Financial Services',
        role: 'Data Scientist',
        tech: ['Python', 'Apache Spark', 'ETL', 'API Development'],
        description: 'Implemented ETL pipeline for client bank compliance systems.',
        achievements: [
            'Built scalable data pipeline in Python and Spark',
            'Developed API payloads to in-house services',
            'Processed large volumes of financial data',
            'Ensured data quality and compliance standards'
        ]
    },
    nlp: {
        title: 'NLP Entity Extraction System',
        role: 'Data Scientist',
        tech: ['Python', 'NLP', 'TensorFlow', 'Entity Recognition'],
        description: 'Used NLP algorithms to extract key entities and relationships from client data.',
        achievements: [
            'Improved solve rates in financial compliance alerts',
            'Extracted entities and relationships automatically',
            'Reduced manual data processing time',
            'Enhanced data quality for downstream systems'
        ]
    },
    tableau: {
        title: 'Litigation Data Tableau Dashboards',
        role: 'Technology Analyst Intern',
        tech: ['Tableau', 'Data Visualization', 'Analytics', 'Business Intelligence'],
        description: 'Built Tableau dashboards showing trends and analysis from litigation data.',
        achievements: [
            'Spearheaded change from Excel to Tableau',
            'Reduced man-hour requirements by 30%',
            'Improved client deliverables efficiency',
            'Created interactive trend analysis dashboards'
        ]
    }
};

const buildings = document.querySelectorAll('.building');
const modal = document.getElementById('projectModal');
const modalBody = modal.querySelector('.modal-body');
const modalClose = modal.querySelector('.modal-close');

buildings.forEach(building => {
    building.addEventListener('click', () => {
        const projectKey = building.dataset.project;
        const project = projectData[projectKey];

        modalBody.innerHTML = `
            <h2 style="font-size: 2.5rem; margin-bottom: 1rem; background: linear-gradient(135deg, var(--primary-color), var(--secondary-color)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;">${project.title}</h2>
            <p style="color: var(--text-dim); font-size: 1.2rem; margin-bottom: 2rem;">${project.role}</p>

            <div style="margin-bottom: 2rem;">
                <h3 style="color: var(--primary-color); margin-bottom: 0.5rem;">Technologies</h3>
                <div style="display: flex; gap: 0.5rem; flex-wrap: wrap;">
                    ${project.tech.map(t => `<span style="padding: 0.5rem 1rem; background: rgba(0, 212, 255, 0.1); border: 1px solid var(--primary-color); border-radius: 20px; font-size: 0.9rem;">${t}</span>`).join('')}
                </div>
            </div>

            <div style="margin-bottom: 2rem;">
                <h3 style="color: var(--primary-color); margin-bottom: 0.5rem;">Description</h3>
                <p style="color: var(--text-dim); line-height: 1.8;">${project.description}</p>
            </div>

            <div>
                <h3 style="color: var(--primary-color); margin-bottom: 0.5rem;">Key Achievements</h3>
                <ul style="list-style: none; padding: 0;">
                    ${project.achievements.map(a => `<li style="padding: 0.5rem 0; padding-left: 1.5rem; position: relative; color: var(--text-dim);"><span style="position: absolute; left: 0; color: var(--primary-color);">→</span>${a}</li>`).join('')}
                </ul>
            </div>
        `;

        modal.classList.add('active');
    });
});

modalClose.addEventListener('click', () => {
    modal.classList.remove('active');
});

modal.addEventListener('click', (e) => {
    if (e.target === modal) {
        modal.classList.remove('active');
    }
});

// =======================
// SKILLS DESK INTERACTIONS
// =======================

const skillsData = {
    programming: {
        title: 'Programming Languages',
        skills: ['Python', 'SQL', 'Java', 'JavaScript', 'Bash/Shell']
    },
    cloud: {
        title: 'Cloud & Infrastructure',
        skills: ['AWS', 'Tencent Cloud', 'Cloud Architecture', 'Scalable Systems', 'Distributed Computing']
    },
    'data-eng': {
        title: 'Data Engineering',
        skills: ['Apache Spark', 'Apache Kafka', 'Apache Flink', 'ETL Pipelines', 'Data Processing', 'Web Scraping', 'Stream Processing']
    },
    ml: {
        title: 'Machine Learning & AI',
        skills: ['TensorFlow', 'PyTorch', 'Large Language Models', 'NLP', 'Model Fine-tuning', 'Deep Learning', 'Computer Vision']
    },
    tools: {
        title: 'Tools & Visualization',
        skills: ['Tableau', 'Microsoft BI', 'Qlikview', 'Git', 'Docker', 'Kubernetes', 'Data Validation']
    },
    databases: {
        title: 'Databases',
        skills: ['PostgreSQL', 'MySQL', 'Trino', 'TiDB', 'Database Design', 'Query Optimization', 'NoSQL']
    }
};

const deskItems = document.querySelectorAll('.desk-item');
const panel = document.getElementById('skillPanel');
const panelContent = panel.querySelector('.panel-content');
const panelClose = panel.querySelector('.panel-close');

deskItems.forEach(item => {
    item.addEventListener('click', () => {
        const skillKey = item.dataset.skill;
        const skillSet = skillsData[skillKey];

        panelContent.innerHTML = `
            <h2 style="font-size: 2.5rem; margin-bottom: 2rem; background: linear-gradient(135deg, var(--primary-color), var(--secondary-color)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;">${skillSet.title}</h2>

            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; max-width: 700px; margin: 0 auto;">
                ${skillSet.skills.map(skill => `
                    <div style="padding: 1.5rem; background: linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(184, 79, 255, 0.1)); border: 2px solid rgba(255, 255, 255, 0.2); border-radius: 15px; text-align: center; font-weight: 600; transition: all 0.3s ease;"
                         onmouseover="this.style.borderColor='var(--primary-color)'; this.style.transform='translateY(-5px)'; this.style.boxShadow='0 10px 30px rgba(0, 212, 255, 0.4)';"
                         onmouseout="this.style.borderColor='rgba(255, 255, 255, 0.2)'; this.style.transform='translateY(0)'; this.style.boxShadow='none';">
                        ${skill}
                    </div>
                `).join('')}
            </div>
        `;

        panel.classList.add('active');
    });
});

panelClose.addEventListener('click', () => {
    panel.classList.remove('active');
});

panel.addEventListener('click', (e) => {
    if (e.target === panel) {
        panel.classList.remove('active');
    }
});

// =======================
// JOURNEY TIMELINE SCROLL
// =======================

const journeyPath = document.querySelector('.journey-path');

// Auto-scroll hint
if (journeyPath) {
    let scrollHintShown = false;

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting && !scrollHintShown) {
                scrollHintShown = true;
                // Slight auto-scroll to hint it's scrollable
                setTimeout(() => {
                    journeyPath.scrollBy({ left: 100, behavior: 'smooth' });
                    setTimeout(() => {
                        journeyPath.scrollBy({ left: -100, behavior: 'smooth' });
                    }, 800);
                }, 500);
            }
        });
    });

    observer.observe(journeyPath);
}

// =======================
// CONTACT ROCKET ANIMATION
// =======================

const rocket = document.querySelector('.rocket');

if (rocket) {
    rocket.addEventListener('click', () => {
        rocket.style.animation = 'none';
        setTimeout(() => {
            rocket.style.animation = 'rocketLaunch 2s ease-out forwards';
        }, 10);
    });
}

// Add rocket launch animation
const style = document.createElement('style');
style.textContent = `
    @keyframes rocketLaunch {
        0% { transform: translateX(-50%) translateY(0) rotate(0deg); }
        100% { transform: translateX(-50%) translateY(-1000px) rotate(10deg); opacity: 0; }
    }
`;
document.head.appendChild(style);

// =======================
// CURSOR GLOW SIZE CHANGE
// =======================

document.addEventListener('click', () => {
    const cursorGlow = document.querySelector('.cursor-glow');
    cursorGlow.style.width = '600px';
    cursorGlow.style.height = '600px';

    setTimeout(() => {
        cursorGlow.style.width = '400px';
        cursorGlow.style.height = '400px';
    }, 300);
});

// =======================
// CONSOLE EASTER EGG
// =======================

console.log('%c 🚀 WELCOME TO MY INTERACTIVE WORLD ', 'background: linear-gradient(135deg, #00d4ff, #b84fff); color: white; padding: 15px; font-size: 16px; font-weight: bold; border-radius: 5px;');
console.log('%c Navigate with the compass below → ', 'color: #00d4ff; font-size: 14px; font-weight: bold;');
console.log('%c Built with: Vanilla JS, Canvas API, CSS Animations ', 'color: #94a3b8; font-size: 12px;');
