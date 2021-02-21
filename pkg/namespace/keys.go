package namespace

// generateMetaKey generates the Namespace Meta key with the proper Prefix.
func generateMetaKey(name string) []byte {
	return []byte(MetaPrefix + "." + name)
}

// generateMetaKey generates the Namespace Public key with the proper Prefix.
func generateNamespaceKey(name, key string) []byte {
	return []byte(NamespacePrefix + "." + name + "." + key)
}
