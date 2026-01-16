# Portfolio Website - Data/Software Engineer

A modern, responsive, and impressive portfolio website designed specifically for Data Engineers and Software Engineers. Features smooth animations, dark/light theme toggle, and a clean professional design.

## Features

- **Responsive Design**: Fully responsive layout that works on all devices (desktop, tablet, mobile)
- **Dark/Light Theme**: Toggle between dark and light themes with persistent preference storage
- **Smooth Animations**: Engaging animations and transitions throughout the site
- **Interactive Sections**:
  - Hero section with typing animation
  - About section with statistics
  - Skills showcase with categorized technologies
  - Filterable projects gallery
  - Timeline-based experience section
  - Contact form with validation
- **Modern UI/UX**: Clean, professional design with gradient accents
- **Performance Optimized**: Fast loading and smooth scrolling
- **SEO Friendly**: Semantic HTML structure

## Sections

1. **Home/Hero**: Eye-catching introduction with typing effect displaying different roles
2. **About**: Personal introduction with key statistics
3. **Skills**: Categorized technical skills (Programming, Data Engineering, ML, Cloud, Databases, Web Dev)
4. **Projects**: Filterable project showcase with categories (All, Data Engineering, ML, Web Development)
5. **Experience**: Timeline view of work history
6. **Contact**: Contact form and information
7. **Footer**: Social links and copyright

## Customization Guide

### 1. Personal Information

Open [index.html](index.html) and update:

- **Line 40**: Replace `Your Name` with your actual name
- **Line 48**: Update the hero description
- **Lines 56-65**: Update social media links:
  - GitHub: Replace `yourusername` with your GitHub username
  - LinkedIn: Replace `yourusername` with your LinkedIn username
  - Twitter: Replace `yourusername` with your Twitter handle
  - Email: Replace `your.email@example.com` with your email

### 2. About Section

Update [index.html](index.html):

- **Lines 93-103**: Update the about text with your personal story
- **Lines 105-117**: Modify statistics (years of experience, projects, technologies)
- **Line 86**: Replace placeholder image with your profile picture

### 3. Skills

Customize the skill tags in each category ([index.html](index.html) lines 127-213):

- Programming Languages
- Data Engineering tools
- Machine Learning frameworks
- Cloud & DevOps platforms
- Databases
- Web Development technologies

Add or remove `<span class="skill-tag">Technology</span>` as needed.

### 4. Projects

Update project cards ([index.html](index.html) lines 224-350):

For each project:
- Replace placeholder images
- Update project titles
- Modify descriptions
- Update technology tags
- Add links to live demos and GitHub repositories
- Adjust `data-category` attribute for filtering (data, ml, or web)

### 5. Experience

Edit the timeline items ([index.html](index.html) lines 363-424):

For each position:
- Update date range
- Change job title
- Modify company name
- Update description and achievements

### 6. Contact Information

Update [index.html](index.html) lines 435-465:

- Email address
- Phone number
- Location
- Contact form (can be integrated with backend service like Formspree, EmailJS, etc.)

### 7. Colors and Branding

Customize the color scheme in [styles.css](styles.css) (lines 2-16):

```css
:root {
    --primary-color: #3b82f6;    /* Main brand color */
    --secondary-color: #8b5cf6;  /* Secondary brand color */
    --accent-color: #06b6d4;     /* Accent color */
    /* ... other variables */
}
```

### 8. Typing Animation

Edit the roles displayed in the typing animation ([script.js](script.js) lines 38-44):

```javascript
const textArray = [
    'Data Engineer',
    'Software Engineer',
    'ML Engineer',
    'Full Stack Developer',
    'Problem Solver'
];
```

## Installation & Setup

1. **Clone or Download** this repository to your local machine

2. **Replace Images**:
   - Add your profile picture (recommended: 400x400px)
   - Add project screenshots (recommended: 600x400px)
   - Update image paths in HTML

3. **Customize Content**: Follow the customization guide above

4. **Open Locally**: Simply open `index.html` in your web browser

5. **Deploy**:
   - **GitHub Pages**: Push to a GitHub repository and enable GitHub Pages
   - **Netlify**: Drag and drop the folder to Netlify
   - **Vercel**: Import the project to Vercel
   - **Traditional Hosting**: Upload all files to your web server

## File Structure

```
portfolio_website/
│
├── index.html          # Main HTML file
├── styles.css          # All styling and animations
├── script.js           # JavaScript functionality
└── README.md          # Documentation (this file)
```

## Technologies Used

- **HTML5**: Semantic markup
- **CSS3**: Modern styling with CSS Grid, Flexbox, animations
- **JavaScript**: Vanilla JS for interactivity
- **Font Awesome**: Icons (loaded via CDN)

## Browser Support

- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)
- Mobile browsers

## Form Integration (Optional)

The contact form currently shows a notification on submit. To make it functional:

### Option 1: Formspree

1. Sign up at [Formspree](https://formspree.io/)
2. Update the form tag in [index.html](index.html):
```html
<form action="https://formspree.io/f/YOUR_FORM_ID" method="POST" class="contact-form" id="contactForm">
```

### Option 2: EmailJS

1. Sign up at [EmailJS](https://www.emailjs.com/)
2. Follow their integration guide
3. Update [script.js](script.js) form submission handler

### Option 3: Custom Backend

Integrate with your own backend API by modifying the form submission handler in [script.js](script.js).

## Performance Tips

1. **Optimize Images**:
   - Compress images before uploading
   - Use WebP format for better compression
   - Implement lazy loading for images

2. **Minimize CSS/JS**:
   - Use minification tools before deployment
   - Consider combining files

3. **CDN**:
   - Font Awesome is loaded from CDN
   - Consider using a CDN for better global performance

## License

Feel free to use this template for your personal portfolio. Attribution is appreciated but not required.

## Support

If you encounter any issues or have questions:
1. Check the customization guide above
2. Review the code comments
3. Ensure all files are in the same directory

## Future Enhancements

Consider adding:
- Blog section
- Testimonials
- Certifications showcase
- Resume download button
- Analytics integration (Google Analytics)
- More interactive animations
- Portfolio CMS integration

---

**Good luck with your portfolio!** Remember to keep it updated with your latest projects and achievements.
