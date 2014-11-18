GEMSPEC = Gem::Specification.new do |s|
    s.name = "ess4r"
    s.version = "0.1"
    s.authors = ["Adam Gardiner"]
    s.date = "2014-11-17"
    s.summary = "Ess4R is a library for interacting with Hyperion Essbase"
    s.description = <<-EOQ
        Ess4R is a Ruby wrapper over the Java API for Oracle's Hyperion Essbase.
        It simplifies common tasks, and smooths some of the rough edges of the Java
        API.
    EOQ
    s.email = "adam.b.gardiner@gmail.com"
    s.homepage = 'https://github.com/agardiner/ess4r'
    s.require_paths = ['lib']
    s.files = ['README.md', 'LICENSE'] + Dir['lib/**/*.rb']
end
