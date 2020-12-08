Embulk::JavaPlugin.register_input(
  "pardot", "org.embulk.input.pardot.PardotInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
